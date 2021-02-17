package main

import (
	"fmt"

	"github.com/dfuse-io/dkafka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var DebugCmd = &cobra.Command{
	Use:   "debug",
	Short: "",
	Long:  "",
}

var CursorCmd = &cobra.Command{
	Use:   "cursor",
	Short: "",
	Long:  "",
}

var CursorReadCmd = &cobra.Command{
	Use:   "read",
	Short: "",
	Long:  "",
	RunE:  cursorReadE,
}

var CursorDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "",
	Long:  "",
	RunE:  cursorDeleteE,
}

var CursorWriteCmd = &cobra.Command{
	Use:   "write",
	Short: "",
	Long:  "",
	RunE:  cursorWriteE,
}

var DebugWriteCmd = &cobra.Command{
	Use:   "write",
	Short: "",
	Long:  "",
	RunE:  debugWriteE,
}

var DebugReadCmd = &cobra.Command{
	Use:   "read",
	Short: "",
	Long:  "",
	RunE:  debugReadE,
}

func init() {
	RootCmd.AddCommand(DebugCmd)

	DebugCmd.AddCommand(DebugWriteCmd)
	DebugCmd.AddCommand(DebugReadCmd)

	DebugWriteCmd.Flags().String("key", "debug", "key to write in kafka")
	DebugWriteCmd.Flags().String("value", "{\"from\":\"dkafka\"}", "value to write in kafka")

	DebugReadCmd.Flags().Int("values", 5, "number of values to read from kafka")
	DebugReadCmd.Flags().Int("offset", -1, "if >= 0, set this value as starting offset")
	DebugReadCmd.Flags().String("group-id", "dkafkadebug", "group ID to use as consumer")

	RootCmd.AddCommand(CursorCmd)
	CursorCmd.AddCommand(CursorReadCmd)
	CursorCmd.AddCommand(CursorDeleteCmd)
	CursorCmd.AddCommand(CursorWriteCmd)

}

func getDkafkaConf() *dkafka.Config {
	return &dkafka.Config{
		KafkaEndpoints:         viper.GetString("global-kafka-endpoints"),
		KafkaSSLEnable:         viper.GetBool("global-kafka-ssl-enable"),
		KafkaSSLCAFile:         viper.GetString("global-kafka-ssl-ca-file"),
		KafkaSSLAuth:           viper.GetBool("global-kafka-ssl-auth"),
		KafkaSSLClientCertFile: viper.GetString("global-kafka-ssl-client-cert-file"),
		KafkaSSLClientKeyFile:  viper.GetString("global-kafka-ssl-client-key-file"),
		KafkaTopic:             viper.GetString("global-kafka-topic"),
		KafkaTransactionID:     viper.GetString("global-kafka-transaction-id"),

		KafkaCursorTopic:           viper.GetString("global-kafka-cursor-topic"),
		KafkaCursorPartition:       int32(viper.GetUint32("global-kafka-cursor-partition")),
		KafkaCursorConsumerGroupID: viper.GetString("global-kafka-cursor-consumer-group-id"),
	}
}

func debugWriteE(cmd *cobra.Command, args []string) error {
	SetupLogger()

	conf := getDkafkaConf()
	key := viper.GetString("debug-write-cmd-key")
	value := viper.GetString("debug-write-cmd-value")

	zlog.Info("writing debug value to kafka", zap.Reflect("config", conf), zap.String("key", key), zap.String("value", value))
	cmd.SilenceUsage = true
	debugger := dkafka.NewDebugger(conf)
	return debugger.Write(key, value)
}

func debugReadE(cmd *cobra.Command, args []string) error {
	SetupLogger()

	conf := getDkafkaConf()
	values := viper.GetInt("debug-read-cmd-values")
	offset := viper.GetInt("debug-read-cmd-offset")
	groupID := viper.GetString("debug-read-cmd-group-id")

	zlog.Info("reading debug values from kafka", zap.Reflect("config", conf), zap.String("group_id", groupID), zap.Int("values", values), zap.Int("offset", offset))
	cmd.SilenceUsage = true
	debugger := dkafka.NewDebugger(conf)
	return debugger.Read(groupID, values, offset)
}

func cursorReadE(cmd *cobra.Command, args []string) error {
	SetupLogger()

	conf := getDkafkaConf()

	zlog.Info("reading cursor from kafka", zap.Reflect("config", conf))
	cmd.SilenceUsage = true
	debugger := dkafka.NewDebugger(conf)
	return debugger.ReadCursor()
}

func cursorWriteE(cmd *cobra.Command, args []string) error {
	SetupLogger()

	conf := getDkafkaConf()
	if len(args) != 1 {
		return fmt.Errorf("cursor write command requires exactly one argument: cursorvalue")
	}

	zlog.Info("writing cursor value from kafka", zap.Reflect("config", conf), zap.String("cursor", args[0]))
	cmd.SilenceUsage = true
	debugger := dkafka.NewDebugger(conf)
	return debugger.WriteCursor(args[0])
}
func cursorDeleteE(cmd *cobra.Command, args []string) error {
	SetupLogger()

	conf := getDkafkaConf()

	zlog.Info("reading debug values from kafka", zap.Reflect("config", conf))
	cmd.SilenceUsage = true
	debugger := dkafka.NewDebugger(conf)
	return debugger.DeleteCursor()
}
