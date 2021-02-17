package dkafka

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

type Debugger struct {
	config *Config
}

func NewDebugger(config *Config) *Debugger {
	return &Debugger{
		config: config,
	}
}

func (d *Debugger) Write(key, val string) error {
	conf := createKafkaConfig(d.config)
	producer, err := getKafkaProducer(conf, d.config.KafkaTransactionID)
	if err != nil {
		return fmt.Errorf("getting kafka producer: %w", err)
	}

	s, err := getKafkaSender(producer, &nilCheckpointer{}, d.config.KafkaTransactionID != "")
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(key),
		Value: []byte(val),
		TopicPartition: kafka.TopicPartition{
			Topic: &d.config.KafkaTopic,
		},
	}
	fmt.Printf("sending message: %s:%s to topic %s\n", key, val, d.config.KafkaTopic)
	if err := s.Send(&msg); err != nil {
		return fmt.Errorf("sending message: %w", err)
	}

	if err := s.Commit(context.Background(), ""); err != nil {
		return fmt.Errorf("committing message: %w", err)
	}
	s.producer.Close()
	return nil
}

func (d *Debugger) Read(groupID string, numValues int, startOffset int) error {
	conf := createKafkaConfig(d.config)

	conf["group.id"] = groupID
	//conf["auto.offset.reset"] = "smallest"
	consumer, err := kafka.NewConsumer(&conf)
	if err != nil {
		return fmt.Errorf("creating consumer: %w", err)
	}

	defer func() {
		if err := consumer.Unsubscribe(); err != nil {
			zlog.Error("error unsubscribing consumer", zap.Error(err))
		}
		if err := consumer.Close(); err != nil {
			zlog.Error("error closing consumer", zap.Error(err))
		}
	}()

	consumer.Subscribe(d.config.KafkaTopic, nil)

	//md, err := consumer.GetMetadata(&d.config.KafkaTopic, false, 500)
	//if err != nil {
	//	return fmt.Errorf("getting metadata: %w", err)
	//}
	//fmt.Println("metadata", md)

	if startOffset >= 0 {
		zlog.Debug("setting offset to..", zap.Int("start_offset", startOffset))
		err = consumer.Assign([]kafka.TopicPartition{
			kafka.TopicPartition{
				Topic:  &d.config.KafkaTopic,
				Offset: kafka.Offset(startOffset),
			}})
		if err != nil {
			return fmt.Errorf("assigning topic and offset: %w", err)
		}
	}

	for i := 0; numValues <= 0 || i < numValues; i++ {
		ev := consumer.Poll(1000)
		switch event := ev.(type) {
		case kafka.Error:
			fmt.Printf("got error: %w\n", event)
			return event
		case *kafka.Message:
			fmt.Printf("got event: %+v, key:%s, val:%s (partition: %d)\n", event, string(event.Key), string(event.Value), event.TopicPartition.Partition)
		default:
			if ev == nil {
				fmt.Println("got nil value")
			} else {
				fmt.Println("got unexpected value", ev)
			}
		}
	}

	return nil
}
