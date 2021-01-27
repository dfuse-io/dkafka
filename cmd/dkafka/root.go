package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/dfuse-io/dkafka"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// RootCmd represents the CLI command
var RootCmd = &cobra.Command{
	Use:   "dkafka",
	Short: "",
	Long:  "",
}

var PublishCmd = &cobra.Command{
	Use:   "publish",
	Short: "",
	Long:  "",
	RunE:  publishRunE,
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().String("dfuse-firehose-grpc-addr", "localhost:13035", "firehose endpoint to connect to")
	RootCmd.PersistentFlags().String("dfuse-firehose-include-expr", "", "CEL expression tu use for requests to firehose")
	RootCmd.PersistentFlags().String("dfuse-auth-token", "", "JWT to authenticate to dfuse (empty to skip authentication)")
	RootCmd.PersistentFlags().Bool("dry-run", false, "do not send anything to kafka, just print content")
	RootCmd.PersistentFlags().String("kafka-endpoints", "127.0.0.1:9092", "comma-separated kafka endpoint addresses")
	RootCmd.PersistentFlags().Bool("kafka-ssl-enable", false, "use SSL when connecting to kafka endpoints")
	RootCmd.PersistentFlags().String("kafka-ssl-ca-file", "", "path to certificate authority validating kafka endpoints")
	RootCmd.PersistentFlags().Bool("kafka-ssl-auth", false, "authenticate to kafka endpoints using client certificate (requires {kafka-ssl-enable}")
	RootCmd.PersistentFlags().String("kafka-ssl-client-cert-file", "./client.crt.pem", "path to client certificate to authenticate to kafka endpoint")
	RootCmd.PersistentFlags().String("kafka-ssl-client-key-file", "./client.key.pem", "path to client key to authenticate to kafka endpoint")

	RootCmd.PersistentFlags().String("kafka-topic", "default", "kafka topic to use for all writes or reads")
	RootCmd.PersistentFlags().String("kafka-cursor-topic", "default", "kafka topic to use for all writes or reads")

	RootCmd.PersistentFlags().String("log-format", "text", "Format for logging to stdout. Either 'text' or 'stackdriver'")
	RootCmd.PersistentFlags().CountP("verbose", "v", "Enables verbose output (-vvvv for max verbosity)")
	RootCmd.PersistentFlags().String("log-level-switcher-listen-addr", "localhost:1065", "If non-empty, the process will listen on this address for json-formatted requests to change different logger levels (see DEBUG.md for more info)")
}

func initConfig() {
	viper.SetEnvPrefix("DKAFKA")
	viper.AutomaticEnv()
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)
	recurseViperCommands(RootCmd, nil)
}

func recurseViperCommands(root *cobra.Command, segments []string) {
	// Stolen from: github.com/abourget/viperbind
	var segmentPrefix string
	if len(segments) > 0 {
		segmentPrefix = strings.Join(segments, "-") + "-"
	}

	root.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		newVar := segmentPrefix + "global-" + f.Name
		viper.BindPFlag(newVar, f)
	})
	root.Flags().VisitAll(func(f *pflag.Flag) {
		newVar := segmentPrefix + "cmd-" + f.Name
		viper.BindPFlag(newVar, f)
	})

	for _, cmd := range root.Commands() {
		recurseViperCommands(cmd, append(segments, cmd.Name()))
	}
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
