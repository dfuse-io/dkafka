package dkafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/dfuse-io/dfuse-eosio/filtering"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"go.uber.org/zap"
	"golang.org/x/oauth2"

	"github.com/dfuse-io/shutter"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"

	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type Config struct {
	DfuseGRPCEndpoint string
	DfuseToken        string

	DryRun        bool // do not connect to Kafka, just print to stdout
	BatchMode     bool
	StartBlockNum int64
	StopBlockNum  uint64
	StateFile     string

	KafkaEndpoints         []string
	KafkaSSLEnable         bool
	KafkaSSLCAFile         string
	KafkaSSLInsecure       bool
	KafkaSSLAuth           bool
	KafkaSSLClientCertFile string
	KafkaSSLClientKeyFile  string

	IncludeFilterExpr string
	KafkaTopic        string
	EventSource       string
	EventKeysExpr     string
	EventTypeExpr     string
	EventExtensions   map[string]string
}

type App struct {
	*shutter.Shutter
	config         *Config
	readinessProbe pbhealth.HealthClient
}

func New(config *Config) *App {
	return &App{
		Shutter: shutter.New(),
		config:  config,
	}
}

func (a *App) Run() error {

	var syncProducer sarama.SyncProducer
	if a.config.DryRun {
		prod, err := NewFakeProducer("-")
		if err != nil {
			return fmt.Errorf("cannot start dry-run fake producer: %s", err)
		}
		syncProducer = prod

	} else {
		saramaConfig := sarama.NewConfig()
		saramaConfig.Version = sarama.V2_0_0_0
		saramaConfig.Producer.Return.Successes = true // required for SyncProducer
		if a.config.KafkaSSLEnable {
			saramaConfig.Net.TLS.Enable = true
			tlsConfig, err := tlsConfig(a.config.KafkaSSLCAFile)
			if err != nil {
				return fmt.Errorf("cannot create kafka TLS config: %w", err)
			}
			if a.config.KafkaSSLAuth {
				zlog.Debug("setting kafka SSL auth")
				if err := addClientCert(
					a.config.KafkaSSLClientCertFile,
					a.config.KafkaSSLClientKeyFile,
					tlsConfig,
				); err != nil {
					return fmt.Errorf("cannot load client certs to authenticate to kafka via TLS")
				}
			}
			if a.config.KafkaSSLInsecure {
				zlog.Debug("setting insecure_skip_verify")
				tlsConfig.InsecureSkipVerify = true
			}
			saramaConfig.Net.TLS.Config = tlsConfig

		}
		prod, err := sarama.NewSyncProducer(a.config.KafkaEndpoints, saramaConfig)
		if err != nil {
			return fmt.Errorf("cannot start kafka sync producer: %w", err)
		}
		syncProducer = prod
	}

	sender, err := kafka_sarama.NewSenderFromSyncProducer(a.config.KafkaTopic, syncProducer)
	if err != nil {
		return fmt.Errorf("failed to create protocol: %w", err)
	}
	defer sender.Close(context.Background())

	c, err := cloudevents.NewClient(sender, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		return fmt.Errorf("failed to create client, %w", err)
	}

	addr := a.config.DfuseGRPCEndpoint
	plaintext := strings.Contains(addr, "*")
	addr = strings.Replace(addr, "*", "", -1)
	var dialOptions []grpc.DialOption
	if plaintext {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	} else {
		transportCreds := credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(transportCreds))
		credential := oauth.NewOauthAccess(&oauth2.Token{AccessToken: a.config.DfuseToken, TokenType: "Bearer"})
		dialOptions = append(dialOptions, grpc.WithPerRPCCredentials(credential))
	}
	conn, err := grpc.Dial(addr,
		dialOptions...,
	)
	if err != nil {
		return fmt.Errorf("connecting to grpc address %s: %w", addr, err)
	}

	client := pbbstream.NewBlockStreamV2Client(conn)

	if !a.config.BatchMode {
		return fmt.Errorf("live mode not implemented")
	}
	req := &pbbstream.BlocksRequestV2{
		StartBlockNum:     a.config.StartBlockNum,
		StopBlockNum:      a.config.StopBlockNum,
		ExcludeStartBlock: false,
		Decoded:           true,
		HandleForks:       true,
		IncludeFilterExpr: a.config.IncludeFilterExpr,
	}
	if irreversibleOnly {
		req.HandleForksSteps = []pbbstream.ForkStep{pbbstream.ForkStep_STEP_IRREVERSIBLE}
	}

	ctx := context.Background()
	executor, err := client.Blocks(ctx, req)
	if err != nil {
		return fmt.Errorf("requesting blocks from dfuse firehose: %w", err)
	}

	eventTypeProg, err := exprToCelProgram(a.config.EventTypeExpr)
	if err != nil {
		return fmt.Errorf("cannot parse event-type-expr: %w", err)
	}
	eventKeyProg, err := exprToCelProgram(a.config.EventKeysExpr)
	if err != nil {
		return fmt.Errorf("cannot parse event-keys-expr: %w", err)
	}

	var extensions []*extension
	for k, v := range a.config.EventExtensions {
		prog, err := exprToCelProgram(v)
		if err != nil {
			return fmt.Errorf("cannot parse event-extension: %w", err)
		}
		extensions = append(extensions, &extension{
			name: k,
			expr: v,
			prog: prog,
		})

	}

	for {
		msg, err := executor.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("error on receive: %w", err)
		}

		blk := &pbcodec.Block{}
		if err := ptypes.UnmarshalAny(msg.Block, blk); err != nil {
			return fmt.Errorf("decoding any of type %q: %w", msg.Block.TypeUrl, err)
		}
		zlog.Debug("incoming block", zap.Uint32("blk_number", blk.Number), zap.Int("length_filtered_trx_traces", len(blk.FilteredTransactionTraces)))

		step := sanitizeStep(msg.Step.String())

		for _, trx := range blk.TransactionTraces() {
			status := sanitizeStatus(trx.Receipt.Status.String())
			memoizableTrxTrace := filtering.MemoizableTrxTrace{TrxTrace: trx}
			for _, act := range trx.ActionTraces {
				if !act.FilteringMatched {
					continue
				}
				var jsonData json.RawMessage
				if act.Action.JsonData != "" {
					jsonData = json.RawMessage(act.Action.JsonData)
				}
				activation := filtering.NewActionTraceActivation(
					act,
					memoizableTrxTrace,
					msg.Step.String(),
				)

				var auths []string
				for _, auth := range act.Action.Authorization {
					auths = append(auths, auth.Authorization())
				}

				eosioAction := event{
					BlockNum:      blk.Number,
					BlockID:       blk.Id,
					Status:        status,
					Step:          step,
					TransactionID: trx.Id,
					ActionInfo: ActionInfo{
						Account:        act.Account(),
						Receiver:       act.Receiver,
						Action:         act.Name(),
						JSONData:       &jsonData,
						DBOps:          trx.DBOpsForAction(act.ExecutionIndex),
						Authorization:  auths,
						GlobalSequence: act.Receipt.GlobalSequence,
					},
				}

				eventType, err := evalString(eventTypeProg, activation)
				if err != nil {
					return fmt.Errorf("error eventtype eval: %w", err)
				}

				extensionsKV := make(map[string]string)
				for _, ext := range extensions {
					val, err := evalString(ext.prog, activation)
					if err != nil {
						return fmt.Errorf("program: %w", err)
					}
					extensionsKV[ext.name] = val

				}

				eventKeys, err := evalStringArray(eventKeyProg, activation)
				if err != nil {
					return fmt.Errorf("event keyeval: %w", err)
				}

				dedupeMap := make(map[string]bool)
				for _, eventKey := range eventKeys {
					if dedupeMap[eventKey] {
						continue
					}
					dedupeMap[eventKey] = true
					e := cloudevents.NewEvent()
					e.SetID(hashString(fmt.Sprintf("%s%s%d%s%s", blk.Id, trx.Id, act.ExecutionIndex, msg.Step.String(), eventKey)))
					e.SetType(eventType)
					e.SetSource(a.config.EventSource)
					for k, v := range extensionsKV {
						e.SetExtension(k, v)
					}
					e.SetExtension("datacontenttype", "application/json")
					e.SetExtension("blkstep", step)

					e.SetTime(blk.MustTime())
					_ = e.SetData(cloudevents.ApplicationJSON, eosioAction)

					if result := c.Send(
						kafka_sarama.WithMessageKey(ctx, sarama.StringEncoder(eventKey)),
						e,
					); cloudevents.IsUndelivered(result) {
						zlog.Warn("failed to send", zap.Uint32("blk_number", blk.Number), zap.String("event_id", e.ID()), zap.Error(err))
					} else {
						zlog.Debug("sent event", zap.Uint32("blk_number", blk.Number), zap.String("event_id", e.ID()))
					}
				}

			}
		}
	}
}
