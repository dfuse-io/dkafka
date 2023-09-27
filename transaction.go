package dkafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
)

const transactionNotification = "TransactionNotification"
const transactionNamespace = "io.ultra.dkafka.transaction"

type transactionMetaSupplier struct {
}

func (dms transactionMetaSupplier) GetVersion() string {
	return "1.0.0"
}
func (dms transactionMetaSupplier) GetSource() string {
	return "dkafka-cli"
}
func (dms transactionMetaSupplier) GetDomain() string {
	return "dkafka"
}
func (dms transactionMetaSupplier) GetCompatibility() string {
	return "FORWARD"
}
func (dms transactionMetaSupplier) GetType() string {
	return "notification"
}

var TransactionMessageSchema = MessageSchema{
	transactionSchema,
	newMeta(transactionMetaSupplier{}),
}

var transactionSchema = RecordSchema{
	Type:      "record",
	Name:      transactionNotification,
	Namespace: transactionNamespace,
	Doc:       "Transaction info",
	Fields:    []FieldSchema{
		// TODO
	},
}

type transactionGenerator struct {
	headers  []kafka.Header
	topic    string
	abiCodec ABICodec
}

func (t transactionGenerator) Apply(genContext TransactionContext) ([]*kafka.Message, error) {

	transactionMap := newTransactionMap(genContext.transaction)
	codec, err := t.abiCodec.GetCodec(transactionNotification, 0)
	if err != nil {
		return nil, fmt.Errorf("transactionGenerator.Apply() fail to get codec for %s: %w", transactionNotification, err)
	}
	value, err := codec.Marshal(nil, transactionMap)
	if err != nil {
		return nil, fmt.Errorf("transactionGenerator.Apply() fail to marshal %s: %w", dkafkaCheckpoint, err)
	}
	transactionIdBytes := []byte(genContext.transaction.Id)
	headers := append(t.headers,
		kafka.Header{
			Key:   "ce_id",
			Value: transactionIdBytes,
		},
		kafka.Header{
			Key:   "ce_type",
			Value: []byte(transactionNotification),
		},
		BlockStep{blk: genContext.block}.timeHeader(),
		kafka.Header{
			Key:   "ce_blkstep",
			Value: []byte(genContext.stepName),
		},
		genContext.blockStep.timeHeader(),
		newCursorHeader(genContext.cursor),
		newPreviousCursorHeader(genContext.blockStep.previousCursor),
	)
	headers = append(headers, codec.GetHeaders()...)

	msg := &kafka.Message{
		Key:     transactionIdBytes,
		Headers: headers,
		Value:   value,
		TopicPartition: kafka.TopicPartition{
			Topic:     &t.topic,
			Partition: kafka.PartitionAny,
		},
	}
	return []*kafka.Message{msg}, nil
}

func newTransactionMap(transaction *pbcodec.TransactionTrace) map[string]interface{} {
	return map[string]interface{}{}
}
