package dkafka

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/Shopify/sarama"
)

func NewFakeProducer(filename string) (*fakeSyncProducer, error) {
	if filename == "-" {
		return &fakeSyncProducer{os.Stdout}, nil
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return &fakeSyncProducer{file}, nil
}

type fakeSyncProducer struct {
	writer io.Writer
}

type marshallableMessage struct {
	Headers map[string]string `json:"headers"`
	Key     string            `json:"key"`
	Body    json.RawMessage   `json:"body"`
}

func (p *fakeSyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	mmsg := &marshallableMessage{}
	mmsg.Headers = make(map[string]string)
	for _, h := range msg.Headers {
		mmsg.Headers[string(h.Key)] = string(h.Value)
	}
	keyBytes, err := msg.Key.Encode()
	if err != nil {
		return 0, 0, err
	}
	mmsg.Key = string(keyBytes)

	bodyBytes, err := msg.Value.Encode()
	if err != nil {
		return 0, 0, err
	}

	mmsg.Body = json.RawMessage(bodyBytes)

	data, err := json.Marshal(mmsg)
	if err != nil {
		return
	}
	fmt.Fprintln(p.writer, string(data))
	return

}

func (p *fakeSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	fmt.Println(msgs)
	return nil
}
func (p *fakeSyncProducer) Close() error {
	return nil
}
