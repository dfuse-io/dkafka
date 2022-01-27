package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/dfuse-io/dkafka"
	"github.com/iancoleman/strcase"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var AvroCmd = &cobra.Command{
	Use:   "avro",
	Short: "",
	Long:  "",
}

var AvroSchemaCmd = &cobra.Command{
	Use:   "schema [-n namespace] [-T ce_type] --table table  --action act abi-file-def",
	Short: "Generate avro schema from ABI file definition",
	Long: `Generate avro schema from ABI file definition. The ABI file argument (abi-file-def)
must be in this format:
'{account}:{path/to/filename}' (ex: 'eosio.token:/tmp/eosio_token.abi').`,
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"abi-file-path"},
	RunE:      generateAvroSchema,
}

func init() {
	RootCmd.AddCommand(AvroCmd)

	AvroCmd.AddCommand(AvroSchemaCmd)

	AvroSchemaCmd.Flags().StringP("action", "a", "", "action name on which generates the schema")
	AvroSchemaCmd.Flags().StringP("table", "t", "", "exclusive table added to the db_ops")
	AvroSchemaCmd.Flags().StringP("namespace", "n", "", "namespace of the message")
	AvroSchemaCmd.Flags().StringP("version", "V", "", "version of the schema in a semver form: 1.2.3")
	AvroSchemaCmd.Flags().StringP("type", "T", "", "Optional type of the message otherwise <Account><ActionName>On<TableName>Notification")
	AvroSchemaCmd.Flags().StringP("output-dir", "o", "./", `Optional output directory for the avro schema. The file name pattern is
the schema type in snake-case with '.avsc' extension`)
}

func generateAvroSchema(cmd *cobra.Command, args []string) error {
	SetupLogger()
	action := viper.GetString("avro-schema-cmd-action")
	table := viper.GetString("avro-schema-cmd-table")
	namespace := viper.GetString("avro-schema-cmd-namespace")
	ceType := viper.GetString("avro-schema-cmd-type")
	outputDir := viper.GetString("avro-schema-cmd-output-dir")
	version := viper.GetString("avro-schema-cmd-version")
	abiString := args[0]

	account, abiFile, err := dkafka.ParseABIFileSpec(abiString)
	if err != nil {
		return err
	}

	if _, err := os.Stat(outputDir); err != nil {
		zlog.Fatal("cannot reach", zap.String("output-dir", outputDir), zap.Error(err))
	}

	zlog.Info(
		"generate avro schema with params",
		zap.String("account", account),
		zap.String("table", table),
		zap.String("namespace", namespace),
		zap.String("type", ceType),
		zap.String("version", version),
		zap.String("abi", abiString),
	)
	abi, err := dkafka.LoadABIFile(abiFile)
	if err != nil {
		return err
	}
	abiSpec := dkafka.AbiSpec{
		Account: account,
		Abi:     abi,
	}
	opts := dkafka.AvroSchemaGenOptions{
		Action:    action,
		Table:     table,
		Namespace: namespace,
		Type:      ceType,
		Version:   version,
		AbiSpec:   abiSpec,
	}

	schema, err := dkafka.GenerateSchema(opts)
	if err != nil {
		zlog.Fatal("generation error", zap.Error(err))
	}
	zlog.Debug("dkafka.GenerateSchema()", zap.Any("schema", schema), zap.Error(err))
	fileName := strcase.ToSnake(schema.Name)
	fileName = fmt.Sprintf("%s.avsc", fileName)
	filePath := filepath.Join(outputDir, fileName)
	zlog.Info("save schema", zap.String("path", filePath))
	jsonString, err := json.Marshal(schema)
	if err != nil {
		zlog.Fatal("cannot convert schema to json", zap.Error(err))
	}
	ioutil.WriteFile(filePath, jsonString, 0664)
	if err != nil {
		zlog.Fatal("cannot write schema", zap.String("path", filePath), zap.Error(err))
	}
	return err
}
