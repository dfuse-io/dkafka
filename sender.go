package dkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

type Sender interface {
	Send(ctx context.Context, messages []*kafka.Message, cursor string) error
	SaveCP(ctx context.Context, cursor string) error
}

type DryRunSender struct{}

func (s *DryRunSender) Send(ctx context.Context, messages []*kafka.Message, cursor string) error {
	for i, msg := range messages {
		outjson, err := messageToJSON(msg)
		if err != nil {
			return err
		}
		fmt.Printf("%d: %s", i, string(outjson))
	}
	return nil
}

func (s *DryRunSender) SaveCP(ctx context.Context, cursor string) error {
	return nil
}

type KafkaSender struct {
	producer *kafka.Producer
	cp       checkpointer
}

func (s *KafkaSender) Send(ctx context.Context, messages []*kafka.Message, cursor string) error {
	for _, msg := range messages {
		if err := s.producer.Produce(msg, nil); err != nil {
			return err
		}
	}
	if err := s.cp.Save(cursor); err != nil {
		return err
	}
	return nil
}

func (s *KafkaSender) SaveCP(ctx context.Context, cursor string) error {
	return s.cp.Save(cursor)
}

type TransactionalKafkaSender struct {
	delegate KafkaSender
}

func NewSender(ctx context.Context, producer *kafka.Producer, cp checkpointer, useTransactions bool) (Sender, error) {
	ks := KafkaSender{
		producer: producer,
		cp:       cp,
	}
	if useTransactions {
		if err := producer.InitTransactions(ctx); err != nil {
			return nil, fmt.Errorf("producer.InitTransactions() error: %w", err)
		}
		return &TransactionalKafkaSender{
			delegate: ks,
		}, nil
	}
	return &ks, nil
}

func (s *TransactionalKafkaSender) Send(ctx context.Context, messages []*kafka.Message, cursor string) error {
	if err := s.delegate.producer.BeginTransaction(); err != nil {
		return fmt.Errorf("producer.BeginTransaction() error: %w", err)
	}
	if err := s.delegate.Send(ctx, messages, cursor); err != nil {
		if e := s.delegate.producer.AbortTransaction(ctx); e != nil {
			zlog.Error("fail to call producer.AbortTransaction() on Send() failure", zap.NamedError("send_error", err), zap.Error(e))
		}
		return fmt.Errorf("Send() error: %w", err)
	}
	if err := s.delegate.SaveCP(ctx, cursor); err != nil {
		if e := s.delegate.producer.AbortTransaction(ctx); e != nil {
			zlog.Error("fail to call producer.AbortTransaction() on SaveCP() failure", zap.NamedError("save_cp_error", err), zap.Error(e))
		}
		return fmt.Errorf("SaveCP() error: %w", err)
	}
	if err := s.delegate.producer.CommitTransaction(ctx); err != nil {
		if e := s.delegate.producer.AbortTransaction(ctx); e != nil {
			zlog.Error("fail to call producer.AbortTransaction() on producer.CommitTransaction() failure", zap.NamedError("commit_error", err), zap.Error(e))
		}
		return fmt.Errorf("producer.CommitTransaction() error: %w", err)
	}
	return nil
}

func (s *TransactionalKafkaSender) SaveCP(ctx context.Context, cursor string) error {
	if err := s.delegate.producer.BeginTransaction(); err != nil {
		return fmt.Errorf("producer.BeginTransaction() error: %w", err)
	}
	if err := s.delegate.SaveCP(ctx, cursor); err != nil {
		if e := s.delegate.producer.AbortTransaction(ctx); e != nil {
			zlog.Error("fail to call producer.AbortTransaction() on SaveCP() failure", zap.NamedError("save_cp_error", err), zap.Error(e))
		}
		return fmt.Errorf("SaveCP() error: %w", err)
	}
	if err := s.delegate.producer.CommitTransaction(ctx); err != nil {
		if e := s.delegate.producer.AbortTransaction(ctx); e != nil {
			zlog.Error("fail to call producer.AbortTransaction() on producer.CommitTransaction() failure", zap.NamedError("commit_error", err), zap.Error(e))
		}
		return fmt.Errorf("producer.CommitTransaction() error: %w", err)
	}
	return nil
}

type sender interface {
	Send(msg *kafka.Message) error
	CommitIfAfter(ctx context.Context, cursor string, minimumDelay time.Duration) error
	Commit(ctx context.Context, cursor string) error
}

type kafkaSender struct {
	sync.RWMutex
	lastCommit      time.Time
	trxStarted      bool
	producer        *kafka.Producer
	cp              checkpointer
	useTransactions bool
}

func (s *kafkaSender) Send(msg *kafka.Message) error {
	s.RLock()
	defer s.RUnlock()
	return s.producer.Produce(msg, nil)
}

func (s *kafkaSender) Close(ctx context.Context) {
	if s.useTransactions {
		if err := s.producer.CommitTransaction(ctx); err != nil {
			zlog.Error("cannot commit transaction on close", zap.Error(err))
		}
	}
	s.producer.Close()
}

func (s *kafkaSender) CommitIfAfter(ctx context.Context, cursor string, minimumDelay time.Duration) error {
	if time.Since(s.lastCommit) > minimumDelay {
		zlog.Debug("commiting cursor")
		return s.Commit(ctx, cursor)
	}
	return nil
}

func (s *kafkaSender) Commit(ctx context.Context, cursor string) error {
	s.Lock() // full write lock
	defer s.Unlock()

	if err := s.cp.Save(cursor); err != nil {
		return fmt.Errorf("saving cursor: %w", err)
	}
	s.lastCommit = time.Now()

	if s.useTransactions {
		if err := s.producer.CommitTransaction(ctx); err != nil {
			return fmt.Errorf("committing transaction: %w", err)
		}

		if err := s.producer.BeginTransaction(); err != nil {
			return fmt.Errorf("beginning transaction: %w", err)
		}
	}
	return nil
}

func getKafkaProducer(conf kafka.ConfigMap, name string) (*kafka.Producer, error) {
	producerConfig := cloneConfig(conf)
	if name != "" {
		producerConfig["transactional.id"] = name
	}
	return kafka.NewProducer(&producerConfig)
}

func getKafkaSender(producer *kafka.Producer, cp checkpointer, useTransactions bool) (*kafkaSender, error) {
	if useTransactions {
		ctx := context.Background() //FIXME
		if err := producer.InitTransactions(ctx); err != nil {
			return nil, fmt.Errorf("running InitTransactions: %w", err)
		}

		// initial transaction
		if err := producer.BeginTransaction(); err != nil {
			return nil, fmt.Errorf("running BeginTransaction: %w", err)
		}
	}

	return &kafkaSender{
		cp:              cp,
		producer:        producer,
		useTransactions: useTransactions,
	}, nil
}

type dryRunSender struct{}

type fakeMessage struct {
	Topic     string          `json:"topic"`
	Headers   []string        `json:"headers"`
	Partition int             `json:"partition"`
	Offset    int             `json:"offset"`
	TS        uint64          `json:"ts"`
	Key       string          `json:"key"`
	Payload   json.RawMessage `json:"payload"`
}

func messageToJSON(msg *kafka.Message) (json.RawMessage, error) {
	out := &fakeMessage{
		Payload: json.RawMessage(msg.Value),
		Key:     string(msg.Key),
	}
	for _, h := range msg.Headers {
		out.Headers = append(out.Headers, h.Key, string(h.Value))
	}
	return json.Marshal(out)
}

func (s *dryRunSender) Send(msg *kafka.Message) error {
	outjson, err := messageToJSON(msg)
	if err != nil {
		return err
	}
	fmt.Println(string(outjson))
	return nil
}

func (s *dryRunSender) CommitIfAfter(context.Context, string, time.Duration) error {
	return nil
}

func (s *dryRunSender) Commit(context.Context, string) error {
	return nil
}
