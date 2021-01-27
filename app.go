package dkafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dfuse-io/dfuse-eosio/filtering"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"

	"github.com/golang/protobuf/ptypes"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"

	"github.com/dfuse-io/shutter"
)

type Config struct {
	DfuseGRPCEndpoint string
	DfuseToken        string

	DryRun        bool // do not connect to Kafka, just print to stdout
	BatchMode     bool
	StartBlockNum int64
	StopBlockNum  uint64
	StateFile     string

	KafkaEndpoints         string
	KafkaSSLEnable         bool
	KafkaSSLCAFile         string
	KafkaSSLAuth           bool
	KafkaSSLClientCertFile string
	KafkaSSLClientKeyFile  string

	IncludeFilterExpr    string
	KafkaTopic           string
	KafkaCursorTopic     string
	KafkaCursorPartition int32
	EventSource          string
	EventKeysExpr        string
	EventTypeExpr        string
	EventExtensions      map[string]string
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

	conf := createKafkaConfig(a.config)

	producerTransactionID := fmt.Sprintf("dkafka-%s", a.config.KafkaTopic) // should be unique per dkafka instance
	producer, err := getKafkaProducer(conf, producerTransactionID)
	if err != nil {
		return fmt.Errorf("getting kafka producer: %w", err)
	}

	var cp checkpointer
	cp = newKafkaCheckpointer(conf, a.config.KafkaCursorTopic, a.config.KafkaCursorPartition, producer)

	//cp = newFileCheckpointer(a.config.CheckpointFilename)

	cursor, err := cp.Load()
	switch err {
	case NoCursorErr:
		zlog.Info("no cursor found, starting from beginning")
	case nil:
		zlog.Info("found cursor", zap.String("cursor", cursor))
	default:
		return fmt.Errorf("error loading cursor: %w", err)
	}

	var s sender
	s, err = getKafkaSender(producer, cp)
	if err != nil {
		return err
	}

	// get and setup the dfuse fetcher that gets a stream of blocks, includes the filter, will include the auth token resolver/refresher
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

	req := &pbbstream.BlocksRequestV2{
		IncludeFilterExpr: a.config.IncludeFilterExpr,
	}

	if a.config.BatchMode {
		req.StartBlockNum = a.config.StartBlockNum
		req.StopBlockNum = a.config.StopBlockNum
	} else {
		// FIXME
		req.StartCursor = "BJrGChD6xWFwlatVsKhV5KWwLpcyB11rXwvlKhFBhdqj9iOTiJSuUmRxOxSFxvj0iRG-SAj6jIrIHHt998lTv9fswOtnuXIxTnIolo_n_LzvePanPwJKd75oXu-JaNjbWzzXYgOvKOcI44bvafeMbhNjZJElLTO2hm5WooBcc_AV6yE2xjn5esrQg_-X9oZGrOpxELKplizwUzB4eho="
	}
	if irreversibleOnly {
		req.ForkSteps = []pbbstream.ForkStep{pbbstream.ForkStep_STEP_IRREVERSIBLE}
	}

	ctx := context.Background()
	executor, err := client.Blocks(ctx, req)
	if err != nil {
		return fmt.Errorf("requesting blocks from dfuse firehose: %w", err)
	}

	// setup the transformer, that will transform incoming blocks

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

	// loop: receive block,  transform block, send message...
	for {
		msg, err := executor.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("error on receive: %w", err)
		}
		fmt.Println(msg.Cursor)

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

					_ = eventType
					msg := kafka.Message{
						Value: eosioAction.JSON(),
						TopicPartition: kafka.TopicPartition{
							Topic: &a.config.KafkaTopic,
						},
					}
					fmt.Println(msg)
					if err := s.Send(&msg); err != nil {
						return fmt.Errorf("sending message: %w", err)
					}

					//e := cloudevents.NewEvent()
					//e.SetID(hashString(fmt.Sprintf("%s%s%d%s%s", blk.Id, trx.Id, act.ExecutionIndex, msg.Step.String(), eventKey)))
					//e.SetType(eventType)
					//e.SetSource(a.config.EventSource)
					//for k, v := range extensionsKV {
					//	e.SetExtension(k, v)
					//}
					//e.SetExtension("datacontenttype", "application/json")
					//e.SetExtension("blkstep", step)

					//e.SetTime(blk.MustTime())
					//_ = e.SetData(cloudevents.ApplicationJSON, eosioAction)

					//if result := c.Send(
					//	kafka_sarama.WithMessageKey(ctx, sarama.StringEncoder(eventKey)),
					//	e,
					//); cloudevents.IsUndelivered(result) {
					//	zlog.Warn("failed to send", zap.Uint32("blk_number", blk.Number), zap.String("event_id", e.ID()), zap.Error(err))
					//} else {
					//	zlog.Debug("sent event", zap.Uint32("blk_number", blk.Number), zap.String("event_id", e.ID()))
					//}
				}

			}
		}
		if err := s.Commit(context.Background(), msg.Cursor); err != nil {
			fmt.Println("hey error", err)
		}
	}
}

func createKafkaConfig(appConf *Config) kafka.ConfigMap {
	conf := kafka.ConfigMap{
		"bootstrap.servers": appConf.KafkaEndpoints,
	}
	if appConf.KafkaSSLEnable {
		conf["security.protocol"] = "ssl"
		conf["ssl.ca.location"] = appConf.KafkaSSLCAFile
	}
	if appConf.KafkaSSLAuth {
		conf["ssl.certificate.location"] = appConf.KafkaSSLClientCertFile
		conf["ssl.key.location"] = appConf.KafkaSSLClientKeyFile
		//conf["ssl.key.password"] = "keypass"
	}
	return conf
}
