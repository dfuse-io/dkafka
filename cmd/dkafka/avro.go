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
	Short: "Avro utilities",
	Long:  "Avro utilities",
}

var AvroSchemaCmd = &cobra.Command{
	Use:   "schema [-n namespace] [-T ce_type] --table table  --action act abi-file-def",
	Short: "Generate filtered avro schema from ABI file definition",
	Long: `Generate avro schema from ABI file definition. The ABI file argument (abi-file-def)
must be in this format:
'{account}:{path/to/filename}' (ex: 'eosio.token:/tmp/eosio_token.abi').`,
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"abi-file-path"},
	RunE:      generateAvroSchema,
}

var AvroGen = &cobra.Command{
	Use:   "gen",
	Short: "Specialized schema(s) generations",
	Long:  "Specialized Schema(s) generations",
}

var AvroActionSchemaCmd = &cobra.Command{
	Use:   "action [-n namespace] [-V version] -n action-name  abi-file-def",
	Short: "Generate Action message avro schema from ABI file definition",
	Long: `Generate Action message avro schema from ABI file definition. The ABI file argument (abi-file-def)
must be in this format:
'{account}:{path/to/filename}' (ex: 'eosio.token:/tmp/eosio_token.abi').
The schema type (CamelCase) is: <ActionName>Notification`,
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"abi-file-path"},
	Run:       generateActionAvroSchema,
}

var AvroTableSchemaCmd = &cobra.Command{
	Use:   "table [-n namespace] [-V version] -n table-name abi-file-def",
	Short: "Generate Table message avro schema from ABI file definition",
	Long: `Generate Table message avro schema from ABI file definition. The ABI file argument (abi-file-def)
must be in this format:
'{account}:{path/to/filename}' (ex: 'eosio.token:/tmp/eosio_token.abi').
The schema type (CamelCase) is: <TableName>Notification`,
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"abi-file-path"},
	Run:       generateTableAvroSchema,
}

var AvroAllSchemaCmd = &cobra.Command{
	Use:   "all [-n namespace] [-V version] abi-file-def",
	Short: "Generate all tables and actions messages avro schemas from ABI file definition",
	Long: `Generate all tables and actions messages avro schemas from ABI file definition. The ABI file argument (abi-file-def)
must be in this format:
'{account}:{path/to/filename}' (ex: 'eosio.token:/tmp/eosio_token.abi').
The schema type (CamelCase) is for: 
- table: <TableName>Notification
- action: <ActionName>Notification`,
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"abi-file-path"},
	Run:       generateAllAvroSchema,
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

	AvroCmd.AddCommand(AvroGen)
	AvroGen.PersistentFlags().StringP("namespace", "n", "", "namespace of the schema(s). Default: account name")
	AvroGen.PersistentFlags().StringP("version", "V", "", "Optional but strongly recommended version of the schema(s) in a semver form: 1.2.3.")
	AvroGen.PersistentFlags().StringP("output-dir", "o", "./", `Optional output directory for the avro schema. The file name pattern is
	the <account>-<schema-type>.avsc in snake-case.`)

	AvroGen.AddCommand(AvroActionSchemaCmd)
	AvroActionSchemaCmd.Flags().StringP("name", "N", "", "action name on which generates the schema")

	AvroGen.AddCommand(AvroTableSchemaCmd)
	AvroTableSchemaCmd.Flags().StringP("name", "N", "", "table name on which generates the schema")

	AvroGen.AddCommand(AvroAllSchemaCmd)
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
	err = saveSchema(schema, "", outputDir)
	if err != nil {
		zlog.Fatal("saveSchema()", zap.Error(err))
	}
	return nil
}

type GenOptions struct {
	namespace string
	version   string
	outputDir string
	abiSpec   dkafka.AbiSpec
}

func generateActionAvroSchema(cmd *cobra.Command, args []string) {
	SetupLogger()
	opts, err := genOptions(cmd, args)
	if err != nil {
		zlog.Fatal("action generetion fail", zap.Error(err))
	}
	name := viper.GetString("avro-gen-action-cmd-name")
	err = doGenActionAvroSchema(name, opts)
	if err != nil {
		zlog.Fatal("doGenActionAvroSchema()", zap.Error(err))
	}
}

func generateTableAvroSchema(cmd *cobra.Command, args []string) {
	SetupLogger()
	opts, err := genOptions(cmd, args)
	if err != nil {
		zlog.Fatal("action generetion fail", zap.Error(err))
	}
	name := viper.GetString("avro-gen-table-cmd-name")

	err = doGenTableAvroSchema(name, opts)
	if err != nil {
		zlog.Fatal("doGenActionAvroSchema()", zap.Error(err))
	}
}

func generateAllAvroSchema(cmd *cobra.Command, args []string) {
	SetupLogger()
	opts, err := genOptions(cmd, args)
	if err != nil {
		zlog.Fatal("action generetion fail", zap.Error(err))
	}
	zlog.Info("generate all schemas for:", zap.String("account", opts.abiSpec.Account))
	zlog.Info("generate all Actions")
	for _, action := range opts.abiSpec.Abi.Actions {
		err = doGenActionAvroSchema(action.Type, opts)
		if err != nil {
			zlog.Fatal("doGenActionAvroSchema()", zap.Error(err))
		}
	}
	for _, table := range opts.abiSpec.Abi.Tables {
		err = doGenTableAvroSchema(string(table.Name), opts)
		if err != nil {
			zlog.Fatal("doGenTableAvroSchema()", zap.Error(err))
		}

	}
}

func genOptions(cmd *cobra.Command, args []string) (opts GenOptions, err error) {
	namespace := viper.GetString("avro-gen-cmd-namespace")
	outputDir := viper.GetString("avro-gen-cmd-output-dir")
	version := viper.GetString("avro-gen-cmd-version")
	abiString := args[0]

	account, abiFile, err := dkafka.ParseABIFileSpec(abiString)
	if err != nil {
		return
	}

	if _, err = os.Stat(outputDir); err != nil {
		err = fmt.Errorf("cannot reach %s error: %v", outputDir, err)
		return
	}

	zlog.Debug(
		"global avro gen options",
		zap.String("namespace", namespace),
		zap.String("version", version),
		zap.String("outputDir", outputDir),
		zap.String("abi", abiString),
	)

	abi, err := dkafka.LoadABIFile(abiFile)
	if err != nil {
		return
	}
	abiSpec := dkafka.AbiSpec{
		Account: account,
		Abi:     abi,
	}

	return GenOptions{
		namespace: namespace,
		version:   version,
		outputDir: outputDir,
		abiSpec:   abiSpec,
	}, nil
}

func doGenActionAvroSchema(name string, opts GenOptions) error {
	zlog.Info("generate schema for:", zap.String("account", opts.abiSpec.Account), zap.String("action", name))
	return doGenAvroSchema(name, opts, dkafka.GenerateActionSchema)
}

func doGenTableAvroSchema(name string, opts GenOptions) error {
	zlog.Info("generate schema for:", zap.String("account", opts.abiSpec.Account), zap.String("table", name))
	return doGenAvroSchema(name, opts, dkafka.GenerateTableSchema)
}

func doGenAvroSchema(name string, opts GenOptions, f func(dkafka.NamedSchemaGenOptions) (dkafka.Message, error)) error {
	schema, err := f(dkafka.NamedSchemaGenOptions{
		Name:      name,
		Namespace: opts.namespace,
		Version:   opts.version,
		AbiSpec:   opts.abiSpec,
	})
	if err != nil {
		return fmt.Errorf("generation error: %v", err)
	}
	zlog.Debug("dkafka.GenerateActionSchema()", zap.Any("schema", schema), zap.Error(err))
	err = saveSchema(schema, opts.abiSpec.Account, opts.outputDir)
	if err != nil {
		return fmt.Errorf("saveSchema() error: %v", err)
	}
	return nil
}

func saveSchema(schema dkafka.Message, prefix string, outputDir string) error {
	fileName := strcase.ToSnake(fmt.Sprintf("%s%s.avsc", prefix, schema.Name))
	fileName = fmt.Sprintf("%s.avsc", fileName)
	filePath := filepath.Join(outputDir, fileName)
	zlog.Info("save schema", zap.String("path", filePath))
	jsonString, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("cannot convert schema to json error: %v", err)
	}
	ioutil.WriteFile(filePath, jsonString, 0664)
	if err != nil {
		return fmt.Errorf("cannot write schema to '%s', error: %v", filePath, err)
	}
	return nil
}
