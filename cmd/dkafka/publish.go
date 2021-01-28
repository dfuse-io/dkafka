package main

import (
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

	PublishCmd.Flags().StringArray("event-extensions-expr", []string{}, "cloudevent extension definitions in this format: '{key}:{CEL expression}' (ex: 'blk:string(block_num)')")

	PublishCmd.Flags().Bool("batch-mode", false, "Batch mode will ignore cursor and always start from {start-block-num}.")
	PublishCmd.Flags().Int64("start-block-num", 0, "If we are in {batch-mode} or no prior cursor exists, start streaming from this block number (if negative, relative to HEAD)")
	PublishCmd.Flags().Uint64("stop-block-num", 0, "If non-zero, stop processing before this block number")
	PublishCmd.Flags().String("state-file", "./dkafka.state.json", "progress will be saved into this file")

}

func publishRunE(cmd *cobra.Command, args []string) error {
	SetupLogger(&LoggingOptions{
		Verbosity: viper.GetInt("global-verbose") + 2, // FIXME hacking verbosity a bit
		LogFormat: viper.GetString("global-log-format"),
	})

	conf := &dkafka.Config{
		DfuseToken:        viper.GetString("global-dfuse-auth-token"),
		DfuseGRPCEndpoint: viper.GetString("global-dfuse-firehose-grpc-addr"),
		IncludeFilterExpr: viper.GetString("global-dfuse-firehose-include-expr"),

		DryRun:                 viper.GetBool("global-dry-run"),
		KafkaEndpoints:         viper.GetString("global-kafka-endpoints"),
		KafkaSSLEnable:         viper.GetBool("global-kafka-ssl-enable"),
		KafkaSSLCAFile:         viper.GetString("global-kafka-ssl-ca-file"),
		KafkaSSLAuth:           viper.GetBool("global-kafka-ssl-auth"),
		KafkaSSLClientCertFile: viper.GetString("global-kafka-ssl-client-cert-file"),
		KafkaSSLClientKeyFile:  viper.GetString("global-kafka-ssl-client-key-file"),
		KafkaTopic:             viper.GetString("global-kafka-topic"),
		KafkaCursorTopic:       viper.GetString("global-kafka-cursor-topic"),
		KafkaCursorPartition:   int32(viper.GetUint32("global-kafka-cursor-partition")),

		EventSource:   viper.GetString("publish-cmd-event-source"),
		EventKeysExpr: viper.GetString("publish-cmd-event-keys-expr"),
		EventTypeExpr: viper.GetString("publish-cmd-event-type-expr"),
		//	EventExtensions map[string]string //publish-cmd-event-extensions-expr // convert me with `:`

		BatchMode:     viper.GetBool("publish-cmd-batch-mode"),
		StartBlockNum: viper.GetInt64("publish-cmd-start-block-num"),
		StopBlockNum:  viper.GetUint64("publish-cmd-stop-block-num"),
		StateFile:     viper.GetString("publish-cmd-state-file"),
	}

	cmd.SilenceUsage = true
	zlog.Info("starting dkafka publisher", zap.Reflect("config", conf))

	app := dkafka.New(conf)
	return app.Run()
}
