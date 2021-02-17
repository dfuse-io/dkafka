package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/dfuse-io/derr"
	"github.com/dfuse-io/dkafka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var PublishCmd = &cobra.Command{
	Use:   "publish",
	Short: "",
	Long:  "",
	RunE:  publishRunE,
}

func init() {
	RootCmd.AddCommand(PublishCmd)

	PublishCmd.Flags().String("event-source", "dkafka", "custom value for produced cloudevent source")
	PublishCmd.Flags().String("event-keys-expr", "[account]", "CEL expression defining the event keys. More then one key will result in multiple events being sent. Must resolve to an array of strings")
	PublishCmd.Flags().String("event-type-expr", "(notif?'!':'')+account+'/'+action", "CEL expression defining the event type. Must resolve to a string")

	PublishCmd.Flags().StringSlice("event-extensions-expr", []string{}, "cloudevent extension definitions in this format: '{key}:{CEL expression}' (ex: 'blk:string(block_num)')")

	PublishCmd.Flags().Bool("batch-mode", false, "Batch mode will ignore cursor and always start from {start-block-num}.")
	PublishCmd.Flags().Int64("start-block-num", 0, "If we are in {batch-mode} or no prior cursor exists, start streaming from this block number (if negative, relative to HEAD)")
	PublishCmd.Flags().Uint64("stop-block-num", 0, "If non-zero, stop processing before this block number")
	PublishCmd.Flags().String("state-file", "./dkafka.state.json", "progress will be saved into this file")

}

func publishRunE(cmd *cobra.Command, args []string) error {
	SetupLogger()

	extensions := make(map[string]string)
	for _, ext := range viper.GetStringSlice("publish-cmd-event-extensions-expr") {
		kv := strings.SplitN(ext, ":", 2)
		if len(kv) != 2 {
			return fmt.Errorf("invalid value for extension: %s", ext)
		}
		extensions[kv[0]] = kv[1]
	}

	conf := &dkafka.Config{
		DfuseToken:        viper.GetString("global-dfuse-auth-token"),
		DfuseGRPCEndpoint: viper.GetString("global-dfuse-firehose-grpc-addr"),
		IncludeFilterExpr: viper.GetString("global-dfuse-firehose-include-expr"),

		DryRun:                     viper.GetBool("global-dry-run"),
		KafkaEndpoints:             viper.GetString("global-kafka-endpoints"),
		KafkaSSLEnable:             viper.GetBool("global-kafka-ssl-enable"),
		KafkaSSLCAFile:             viper.GetString("global-kafka-ssl-ca-file"),
		KafkaSSLAuth:               viper.GetBool("global-kafka-ssl-auth"),
		KafkaSSLClientCertFile:     viper.GetString("global-kafka-ssl-client-cert-file"),
		KafkaSSLClientKeyFile:      viper.GetString("global-kafka-ssl-client-key-file"),
		KafkaTopic:                 viper.GetString("global-kafka-topic"),
		KafkaCursorTopic:           viper.GetString("global-kafka-cursor-topic"),
		KafkaCursorPartition:       int32(viper.GetUint32("global-kafka-cursor-partition")),
		KafkaCursorConsumerGroupID: viper.GetString("global-kafka-cursor-consumer-group-id"),
		KafkaTransactionID:         viper.GetString("global-kafka-transaction-id"),

		EventSource:     viper.GetString("publish-cmd-event-source"),
		EventKeysExpr:   viper.GetString("publish-cmd-event-keys-expr"),
		EventTypeExpr:   viper.GetString("publish-cmd-event-type-expr"),
		EventExtensions: extensions,

		BatchMode:     viper.GetBool("publish-cmd-batch-mode"),
		StartBlockNum: viper.GetInt64("publish-cmd-start-block-num"),
		StopBlockNum:  viper.GetUint64("publish-cmd-stop-block-num"),
		StateFile:     viper.GetString("publish-cmd-state-file"),
	}

	cmd.SilenceUsage = true
	signalHandler := derr.SetupSignalHandler(time.Second)

	zlog.Info("starting dkafka publisher", zap.Reflect("config", conf))
	app := dkafka.New(conf)
	go func() { app.Shutdown(app.Run()) }()

	select {
	case <-signalHandler:
		app.Shutdown(fmt.Errorf("shutdown signal received"))
	case <-app.Terminating():
	}
	zlog.Info("terminating", zap.Error(app.Err()))

	<-app.Terminated()
	return app.Err()
}
