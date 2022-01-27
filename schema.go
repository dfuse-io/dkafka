package dkafka

import (
	"fmt"
	"strings"

	"github.com/eoscanada/eos-go"
	"github.com/iancoleman/strcase"
	"go.uber.org/zap"
)

type AbiSpec struct {
	Account string
	Abi     *eos.ABI
}

type AvroSchemaGenOptions struct {
	Action    string
	Table     string
	Namespace string
	Type      string
	Version   string
	AbiSpec   AbiSpec
}

func GenerateSchema(options AvroSchemaGenOptions) (Message, error) {
	actionCamelCase := strcase.ToCamel(options.Action)
	tableCamelCase := strcase.ToCamel(options.Table)
	accountCamelCase := strcase.ToCamel(options.AbiSpec.Account)
	baseName := fmt.Sprintf("%s%sOn%s", accountCamelCase, actionCamelCase, tableCamelCase)
	zlog.Debug("ToCamel()", zap.String("baseName", baseName))
	ceType := options.Type
	if ceType == "" {
		ceType = fmt.Sprintf("%sNotification", baseName)
	}
	ceType = strcase.ToCamel(ceType) // to be sure if it's provided by the user
	namespace := strcase.ToDelimited(options.Namespace, '.')
	actionDetailsRecordName := fmt.Sprintf("%sActionInfo", baseName)
	actionParamsRecordName := fmt.Sprintf("%sActionParams", baseName)
	dbOpRecordName := fmt.Sprintf("%sDBOp", baseName)
	dbOpInfoRecordName := fmt.Sprintf("%sDBOpInfo", baseName)

	zlog.Info(
		"generate avro schema with following names for the records",
		zap.String("namespace", namespace),
		zap.String("ce_type", ceType),
		zap.String("actionInfo", actionDetailsRecordName),
		zap.String("actionParams", actionParamsRecordName),
		zap.String("dbOp", dbOpRecordName),
		zap.String("dbOpInfo", dbOpInfoRecordName),
	)

	actionParamsSchema, err := ActionToRecord(options.AbiSpec.Abi, eos.ActionName(options.Action))
	if err != nil {
		return Message{}, err
	}
	actionParamsSchema.Name = actionParamsRecordName
	dbOpSchema, err := TableToRecord(options.AbiSpec.Abi, eos.TableName(options.Table))
	if err != nil {
		return Message{}, err
	}
	dbOpSchema.Name = dbOpRecordName
	dbOpInfoSchema := newDBOpInfoRecord(dbOpInfoRecordName, dbOpSchema)
	schema := newEventSchema(ceType, namespace, options.Version, newActionInfoSchema(actionDetailsRecordName, actionParamsSchema, dbOpInfoSchema))

	return schema, nil
}

func ActionToRecord(abi *eos.ABI, name eos.ActionName) (Record, error) {
	actionDef := abi.ActionForName(name)
	if actionDef == nil {
		return Record{}, fmt.Errorf("action '%s' not found", name)
	}

	return structToRecord(abi, actionDef.Type)
}

func TableToRecord(abi *eos.ABI, name eos.TableName) (Record, error) {
	tableDef := abi.TableForName(name)
	if tableDef == nil {
		return Record{}, fmt.Errorf("table '%s' not found", name)
	}

	return structToRecord(abi, tableDef.Type)
}

func structToRecord(abi *eos.ABI, structName string) (Record, error) {
	s := abi.StructForName(structName)
	if s == nil {
		return Record{}, fmt.Errorf("struct not found: %s", structName)
	}
	fields, err := abiFieldsToRecordFields(abi, s.Fields)
	if err != nil {
		return Record{}, err
	}
	return Record{
		Name:   s.Name,
		Fields: fields,
	}, nil
}

func abiFieldsToRecordFields(abi *eos.ABI, fieldDefs []eos.FieldDef) ([]Field, error) {
	fields := make([]Field, len(fieldDefs))
	for i, fieldDef := range fieldDefs {
		field, err := abiFieldToRecordField(abi, fieldDef)
		if err != nil {
			return fields, err
		}
		fields[i] = field
	}
	return fields, nil
}

func abiFieldToRecordField(abi *eos.ABI, fieldDef eos.FieldDef) (Field, error) {
	zlog.Debug("convert field", zap.String("name", fieldDef.Name), zap.String("type", fieldDef.Type))
	schema, err := resolveFieldTypeSchema(abi, fieldDef.Type)
	if err != nil {
		return Field{}, fmt.Errorf("reslove Field type schema error: %v, on field: %s", err, fieldDef.Name)
	}
	return Field{
		Name: fieldDef.Name,
		Type: schema,
	}, nil
}

/*
   built_in_types.emplace("bool",                      pack_unpack<uint8_t>());
   built_in_types.emplace("int8",                      pack_unpack<int8_t>());
   built_in_types.emplace("uint8",                     pack_unpack<uint8_t>());
   built_in_types.emplace("int16",                     pack_unpack<int16_t>());
   built_in_types.emplace("uint16",                    pack_unpack<uint16_t>());
   built_in_types.emplace("int32",                     pack_unpack<int32_t>());
   built_in_types.emplace("uint32",                    pack_unpack<uint32_t>());
   built_in_types.emplace("int64",                     pack_unpack<int64_t>());
   built_in_types.emplace("uint64",                    pack_unpack<uint64_t>());
   built_in_types.emplace("int128",                    pack_unpack<int128_t>());
   built_in_types.emplace("uint128",                   pack_unpack<uint128_t>());
   built_in_types.emplace("varint32",                  pack_unpack<fc::signed_int>());
   built_in_types.emplace("varuint32",                 pack_unpack<fc::unsigned_int>());

   // TODO: Add proper support for floating point types. For now this is good enough.
   built_in_types.emplace("float32",                   pack_unpack<float>());
   built_in_types.emplace("float64",                   pack_unpack<double>());
   built_in_types.emplace("float128",                  pack_unpack<float128_t>());

   built_in_types.emplace("time_point",                pack_unpack<fc::time_point>());
   built_in_types.emplace("time_point_sec",            pack_unpack<fc::time_point_sec>());
   built_in_types.emplace("block_timestamp_type",      pack_unpack<block_timestamp_type>());

   built_in_types.emplace("name",                      pack_unpack<name>());

   built_in_types.emplace("bytes",                     pack_unpack<bytes>());
   built_in_types.emplace("string",                    pack_unpack<string>());

   built_in_types.emplace("checksum160",               pack_unpack<checksum160_type>());
   built_in_types.emplace("checksum256",               pack_unpack<checksum256_type>());
   built_in_types.emplace("checksum512",               pack_unpack<checksum512_type>());

   built_in_types.emplace("public_key",                pack_unpack_deadline<public_key_type>());
   built_in_types.emplace("signature",                 pack_unpack_deadline<signature_type>());

   built_in_types.emplace("symbol",                    pack_unpack<symbol>());
   built_in_types.emplace("symbol_code",               pack_unpack<symbol_code>());
   built_in_types.emplace("asset",                     pack_unpack<asset>());
   built_in_types.emplace("extended_asset",            pack_unpack<extended_asset>());
*/

var avroPrimitiveTypeByBuiltInTypes map[string]string = map[string]string{
	"bool":                 "boolean",
	"int8":                 "int",
	"uint8":                "int",
	"int16":                "int",
	"uint16":               "int",
	"int32":                "int",
	"uint32":               "long",
	"int64":                "long",
	"uint64":               "long", // FIXME maybe use Decimal here see goavro
	"varint32":             "int",
	"varuint32":            "long",
	"float32":              "float",
	"float64":              "double",
	"time_point":           "long", // FIXME check with blockchain team
	"time_point_sec":       "long", // FIXME check with blockchain team
	"block_timestamp_type": "long", // FIXME check with blockchain team
	"name":                 "string",
	"bytes":                "bytes",
	"string":               "string", // FIXME check with blockchain team
	"checksum160":          "string", // FIXME check with blockchain team
	"checksum256":          "string", // FIXME check with blockchain team
	"checksum512":          "string", // FIXME check with blockchain team
	"public_key":           "string", // FIXME check with blockchain team
	"signature":            "string", // FIXME check with blockchain team
	"symbol":               "string", // FIXME check with blockchain team
	"symbol_code":          "string", // FIXME check with blockchain team
	"asset":                "string", // FIXME check with blockchain team
}

// "int128":    "",
// "uint128":   "",
// "float128",
// "extended_asset",

func resolveFieldTypeSchema(abi *eos.ABI, fieldType string) (Schema, error) {
	zlog.Debug("resolve", zap.String("type", fieldType))
	if elementType := strings.TrimSuffix(fieldType, "[]"); elementType != fieldType {
		// todo array
		zlog.Debug("array of", zap.String("element", elementType))
		itemType, err := resolveFieldTypeSchema(abi, elementType)
		if err != nil {
			return nil, fmt.Errorf("type %s not found, error: %w", elementType, err)
		}
		return NewArray(itemType), nil
	}
	if optionalType := strings.TrimSuffix(fieldType, "?"); optionalType != fieldType {
		zlog.Debug("optional of", zap.String("type", optionalType))
		oType, err := resolveFieldTypeSchema(abi, optionalType)
		if err != nil {
			return nil, fmt.Errorf("type %s not found, error: %w", optionalType, err)
		}
		return NewOptional(oType), nil
	}
	s, err := resolveType(abi, fieldType)
	if err != nil {
		return nil, fmt.Errorf("unknown type: %s, error: %v", fieldType, err)
	}
	return s, nil
}

func resolveType(abi *eos.ABI, name string) (Schema, error) {
	zlog.Debug("find type", zap.String("name", name))
	if primitive, found := avroPrimitiveTypeByBuiltInTypes[name]; found {
		return primitive, nil
	}
	// resolve from types
	if alias, found := abi.TypeNameForNewTypeName(name); found {
		return resolveFieldTypeSchema(abi, alias)
	}
	// resolve from structs
	if record, err := structToRecord(abi, name); err != nil {
		return nil, err
	} else {
		return record, nil
	}
}
