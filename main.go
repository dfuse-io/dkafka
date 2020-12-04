package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"

	eosio "github.com/dfuse-io/dkafka/pb/eosio-codec"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"log"

	"github.com/Shopify/sarama"

	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

//var grpcEndpoint = "blocks.mainnet.eos.dfuse.io:443"
var grpcEndpoint = "localhost:13035"
var dfuseToken = "disabled"
var topic = "quickstart-events"
var kafkaEndpoints = []string{"127.0.0.1:9092"}
var eventType = "dfuse.action_trace"
var eventSource = "dfuse.chain"

func main() {

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_0_0_0

	sender, err := kafka_sarama.NewSender(kafkaEndpoints, saramaConfig, topic)
	if err != nil {
		log.Fatalf("failed to create protocol: %s", err.Error())
	}

	defer sender.Close(context.Background())

	c, err := cloudevents.NewClient(sender, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	addr := grpcEndpoint
	plaintext := strings.Contains(addr, "*")
	addr = strings.Replace(addr, "*", "", -1)
	var dialOptions []grpc.DialOption
	if plaintext {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	} else {
		//		credential := oauth.NewOauthAccess(&oauth2.Token{AccessToken: dfuseToken, TokenType: "Bearer"})
		transportCreds := credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})
		//grpc.WithPerRPCCredentials(credential),
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(transportCreds))
		fmt.Println("connecting to conn with", addr, dialOptions)
	}
	conn, err := grpc.Dial(addr,
		dialOptions...,
	)
	if err != nil {
		panic(err)
	}

	client := pbbstream.NewBlockStreamV2Client(conn)

	req := &pbbstream.BlocksRequestV2{
		StartBlockNum:     10,
		StopBlockNum:      100,
		ExcludeStartBlock: false,
		Decoded:           true,
		HandleForks:       true,
		HandleForksSteps: []pbbstream.ForkStep{
			pbbstream.ForkStep_STEP_IRREVERSIBLE,
		},
		IncludeFilterExpr: "action != 'blah'",
	}

	ctx := context.Background()
	executor, err := client.Blocks(ctx, req)
	if err != nil {
		panic(err)
	}

	for {
		msg, err := executor.Recv()
		if err != nil {
			fmt.Println("exiting on error", err)
			return
		}

		blk := &eosio.Block{}
		if err := ptypes.UnmarshalAny(msg.Block, blk); err != nil {
			fmt.Println(fmt.Errorf("decoding any of type %q: %w", msg.Block.TypeUrl, err))
			return
		}
		log.Printf("getting block %d\n", blk.Number)

		for _, trx := range blk.FilteredTransactionTraces {
			for _, act := range trx.ActionTraces {
				fmt.Println("in trxaction")
				if !act.FilteringMatched {
					fmt.Println("skipping")
					continue
				}
				fmt.Println("not skipping")
				var jsonData json.RawMessage
				if act.Action.JsonData != "" {
					jsonData = json.RawMessage(act.Action.JsonData)
				}
				eosioAction := event{
					BlockNum:      blk.Number,
					BlockID:       blk.Id,
					TransactionID: trx.Id,
					Account:       act.Account(),
					Receiver:      act.Receiver,
					ActionName:    act.Name(),
					JSONData:      &jsonData,
				}
				e := cloudevents.NewEvent()
				e.SetID(fmt.Sprintf("%s-%s-%d", blk.Id, trx.Id, act.ActionOrdinal))
				e.SetType(eventType)
				e.SetSource(eventSource)
				e.SetExtension("datacontenttype", "application/json")
				e.SetTime(blk.MustTime())
				_ = e.SetData(cloudevents.ApplicationJSON, eosioAction)

				if result := c.Send(
					kafka_sarama.WithMessageKey(context.Background(), sarama.StringEncoder(e.ID())),
					e,
				); cloudevents.IsUndelivered(result) {
					log.Printf("failed to send: %v", err)
				} else {
					log.Printf("sent: %d, accepted: %t", blk.Number, cloudevents.IsACK(result))
				}

			}
		}
	}
}

type event struct {
	BlockNum      uint32           `json:"block_num"`
	BlockID       string           `json:"block_id"`
	TransactionID string           `json:"transaction_id"`
	Receiver      string           `json:"receiver"`
	Account       string           `json:"account"`
	ActionName    string           `json:"action_name"`
	JSONData      *json.RawMessage `json:"json_data"`
}

func (e event) JSON() []byte {
	b, _ := json.Marshal(e)
	return b

}
