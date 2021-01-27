package dkafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type sender interface {
	Send(msg *kafka.Message) error
	Commit(ctx context.Context, cursor string) error
}

type kafkaSender struct {
	sync.RWMutex
	trxStarted bool
	producer   *kafka.Producer
	cp         checkpointer
}

func (s *kafkaSender) Send(msg *kafka.Message) error {
	s.RLock()
	defer s.RUnlock()
	return s.producer.Produce(msg, nil)
}

func (s *kafkaSender) Commit(ctx context.Context, cursor string) error {
	s.Lock() // full write lock
	defer s.Unlock()

	if err := s.cp.Save(cursor); err != nil {
		return fmt.Errorf("saving cursor: %w", err)
	}

	if err := s.producer.CommitTransaction(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	if err := s.producer.BeginTransaction(); err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	return nil
}

func getKafkaProducer(conf kafka.ConfigMap, name string) (*kafka.Producer, error) {
	producerConfig := cloneConfig(conf)
	producerConfig["transactional.id"] = name
	return kafka.NewProducer(&producerConfig)
}

func getKafkaSender(producer *kafka.Producer, cp checkpointer) (*kafkaSender, error) {
	ctx := context.Background() //FIXME
	if err := producer.InitTransactions(ctx); err != nil {
		return nil, fmt.Errorf("running InitTransactions: %w", err)
	}

	// initial transaction
	if err := producer.BeginTransaction(); err != nil {
		return nil, fmt.Errorf("running BeginTransaction: %w", err)
	}

	return &kafkaSender{
		cp:       cp,
		producer: producer,
	}, nil
}
