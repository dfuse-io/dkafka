package main

import (
	"fmt"
	"time"

	"github.com/dfuse-io/dkafka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/derr"
	"go.uber.org/zap"
)

var CdCCmd = &cobra.Command{
	Use:   "cdc",
	Short: "Change Data Capture",
	Long:  "Change Data Capture",
}

var CdCTablesCmd = &cobra.Command{
	Use:   dkafka.TABLES_CDC_TYPE,
	Short: "Change Data Capture on smart contract tables",
	Long: `Change Data Capture on smart contract tables.
This argument is the smart contract account to capture.`,
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"account"},
	RunE:      cdcOnTables,
}

var CdCActionsCmd = &cobra.Command{
	Use:   dkafka.ACTIONS_CDC_TYPE,
	Short: "Change Data Capture on smart contract actions",
	Long: `Change Data Capture on smart contract actions.
This argument is the smart contract account to capture.`,
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"account"},
	RunE:      cdcOnActions,
}

func init() {
	RootCmd.AddCommand(CdCCmd)
	CdCCmd.PersistentFlags().Var(compressionTypes, "kafka-compression-type", compressionTypes.Help("Specify the compression type to use for compressing message sets."))
	CdCCmd.PersistentFlags().Int8("kafka-compression-level", int8(-1), `Compression level parameter for algorithm selected by configuration property
kafka-compression-type. Higher values will result in better compression at the
cost of more CPU usage. Usable range is algorithm-dependent: [0-9] for gzip;
[0-12] for lz4; only 0 for snappy; -1 = codec-dependent default compression
level.`)
	CdCCmd.PersistentFlags().Int("kafka-message-max-bytes", 1_000_000, `Maximum Kafka protocol request message size.
Due to different framing overhead between protocol versions the producer is
unable to reliably enforce a strict max message limit at produce time,
the message size is checked against your raw uncompressed message.
The broker will also enforce the the topic's max.message.bytes limit
upon receiving your message. So make sure your brokers configuration
match your producers (same apply for consumers)
(see Apache Kafka documentation).`)

	CdCCmd.PersistentFlags().Bool("batch-mode", false, "Batch mode will ignore cursor and always start from {start-block-num}.")
	CdCCmd.PersistentFlags().Int64("start-block-num", 0, `If we are in {batch-mode} or no prior cursor exists,
start streaming from this block number (if negative, relative to HEAD)`)
	CdCCmd.PersistentFlags().Uint64("stop-block-num", 0, "If non-zero, stop processing before this block number")
	CdCCmd.PersistentFlags().String("state-file", "./dkafka.state.json", "progress will be saved into this file")
	CdCCmd.PersistentFlags().Bool("capture", false, "Activate the capture mode where blocks are saved on the file system in json format.")

	CdCCmd.PersistentFlags().Duration("delay-between-commits", time.Second*10, "no commits to kafka blow this delay, except un shutdown")
	CdCCmd.PersistentFlags().String("event-source", "dkafka", "custom value for produced cloudevent source")

	CdCCmd.PersistentFlags().Bool("executed", false, "Specify publish messages based only on executed actions => modify the state of the blockchain. This remove the error messages")
	CdCCmd.PersistentFlags().Bool("irreversible", false, "Specify publish messages based only on irreversible actions")

	CdCCmd.AddCommand(CdCActionsCmd)
	CdCActionsCmd.Flags().String("actions-expr", "", "A JSON Object that associate the a name of an action to CEL expression for the message key extration.")

	CdCCmd.AddCommand(CdCTablesCmd)
	CdCTablesCmd.Flags().StringSlice("local-abi-files", []string{}, `repeatable, ABI file definition in this format:
'{account}:{path/to/filename}' (ex: 'eosio.token:/tmp/eosio_token.abi').
ABIs are used to decode DB ops. Provided ABIs have highest priority and
will never be fetched or updated`)
	CdCTablesCmd.Flags().String("abicodec-grpc-addr", "", "if set, will connect to this endpoint to fetch contract ABIs")
	CdCTablesCmd.Flags().StringSlice("table-name", []string{}, "the table name on which the message must be sent.")
}

func cdcOnTables(cmd *cobra.Command, args []string) error {
	SetupLogger()
	account := args[0]
	zlog.Debug(
		"CDC on table",
		zap.String("account", account),
	)
	localABIFiles, err := dkafka.ParseABIFileSpecs(viper.GetStringSlice("cdc-tables-cmd-local-abi-files"))
	if err != nil {
		return err
	}

	return executeCdC(cmd, args, dkafka.TABLES_CDC_TYPE, func(c *dkafka.Config) *dkafka.Config {
		c.LocalABIFiles = localABIFiles
		c.ABICodecGRPCAddr = viper.GetString("cdc-tables-cmd-abicodec-grpc-addr")
		c.TableNames = viper.GetStringSlice("cdc-tables-cmd-table-name")
		return c
	})
}

func cdcOnActions(cmd *cobra.Command, args []string) error {
	SetupLogger()
	return executeCdC(cmd, args, dkafka.ACTIONS_CDC_TYPE, func(c *dkafka.Config) *dkafka.Config {
		c.ActionExpressions = viper.GetString("cdc-actions-cmd-actions-expr")
		return c
	})
}

func executeCdC(cmd *cobra.Command, args []string,
	cdcType string, f func(*dkafka.Config) *dkafka.Config) error {
	account := args[0]
	zlog.Debug(
		"CDC on action",
		zap.String("account", account),
	)

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
		KafkaCompressionType:       viper.GetString("cdc-cmd-kafka-compression-type"),
		KafkaCompressionLevel:      viper.GetInt("cdc-cmd-kafka-compression-level"),
		KafkaMessageMaxBytes:       viper.GetInt("cdc-cmd-kafka-message-max-bytes"),
		CommitMinDelay:             viper.GetDuration("cdc-cmd-delay-between-commits"),

		BatchMode:     viper.GetBool("cdc-cmd-batch-mode"),
		StartBlockNum: viper.GetInt64("cdc-cmd-start-block-num"),
		StopBlockNum:  viper.GetUint64("cdc-cmd-stop-block-num"),
		StateFile:     viper.GetString("cdc-cmd-state-file"),
		Capture:       viper.GetBool("cdc-cmd-capture"),

		EventSource: viper.GetString("cdc-cmd-event-source"),

		CdCType:      cdcType,
		Account:      account,
		Irreversible: viper.GetBool("cdc-cmd-irreversible"),
		Executed:     viper.GetBool("cdc-cmd-executed"),
	}
	conf = f(conf)
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
