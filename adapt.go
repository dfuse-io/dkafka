package dkafka

import (
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"go.uber.org/zap"
)

type Adapter interface {
	Adapt(blk *pbcodec.Block, rawStep string) ([]*kafka.Message, error)
}

type CdCAdapter struct {
	topic     string
	saveBlock SaveBlock
	generator Generator2
	// TODO merge all headers
	headers []kafka.Header
}

func (m *CdCAdapter) Adapt(blk *pbcodec.Block, rawStep string) ([]*kafka.Message, error) {
	m.saveBlock(blk)
	step := sanitizeStep(rawStep)
	if blk.Number%100 == 0 {
		zlog.Info("incoming block 1/100", zap.Uint32("blk_number", blk.Number), zap.String("step", step), zap.Int("length_filtered_trx_traces", len(blk.FilteredTransactionTraces)))
	}
	if blk.Number%10 == 0 {
		zlog.Debug("incoming block 1/10", zap.Uint32("blk_number", blk.Number), zap.String("step", step), zap.Int("length_filtered_trx_traces", len(blk.FilteredTransactionTraces)))
	}

	for _, trx := range blk.TransactionTraces() {
		transactionTracesReceived.Inc()
		// manage correlation
		correlation, err := getCorrelation(trx.ActionTraces)
		if err != nil {
			return nil, err
		}
		for _, act := range trx.ActionTraces {
			if !act.FilteringMatched {
				continue
			}
			actionTracesReceived.Inc()
			// generation
			generations, err := m.generator.Apply(GenContext{
				block:       blk,
				stepName:    step,
				transaction: trx,
				actionTrace: act,
				correlation: correlation,
			})

			if err != nil {
				return nil, err
			}
			msgs := make([]*kafka.Message, 0, 1)
			for _, generation := range generations {
				headers := append(m.headers,
					kafka.Header{
						Key:   "ce_id",
						Value: generation.CeId,
					},
					kafka.Header{
						Key:   "ce_type",
						Value: []byte(generation.CeType),
					},
					kafka.Header{
						Key:   "ce_time",
						Value: []byte(blk.MustTime().Format("2006-01-02T15:04:05.9Z")),
					},
					kafka.Header{
						Key:   "ce_blkstep",
						Value: []byte(step),
					},
				)
				// TODO introduce codec
				value, err := json.Marshal(generation.Value)
				if err != nil {
					return nil, err
				}
				msg := &kafka.Message{
					Key:     []byte(generation.Key),
					Headers: headers,
					Value:   value,
					TopicPartition: kafka.TopicPartition{
						Topic:     &m.topic,
						Partition: kafka.PartitionAny,
					},
				}
				msgs = append(msgs, msg)
			}
			return msgs, nil
		}
	}
	return nil, nil
}
