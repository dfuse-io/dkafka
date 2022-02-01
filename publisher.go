package dkafka

import (
	"encoding/json"
)

const dkafkaNamespace = "io.dkafka"

func newCorrelationRecord() Record {
	return newRecordFQN(
		dkafkaNamespace,
		"Correlation",
		[]Field{
			{
				Name: "payer",
				Type: "string",
			},
			{
				Name: "id",
				Type: "string",
			},
		},
	)
}

type Correlation struct {
	Payer string `json:"payer"`
	Id    string `json:"id"`
}

func newActionInfoDetailsSchema(name string, jsonData Record, dbOpsRecord Record) Record {
	return newRecordS(
		name,
		[]Field{
			{
				Name: "payer",
				Type: "string",
			},
			{
				Name: "account",
				Type: "string",
			},
			{
				Name: "receiver",
				Type: "string",
			},
			{
				Name: "action",
				Type: "string",
			},
			{
				Name: "global_seq",
				Type: "long",
			},
			{
				Name: "authorizations",
				Type: NewArray("string"),
			},
			{
				Name: "db_ops",
				Type: NewArray(dbOpsRecord),
			},
			{
				Name: "json_data",
				Type: jsonData,
			},
		},
	)
}

func newDBOpInfoRecord(tableName string, jsonData Record) Record {
	return newRecordS(
		tableName,
		[]Field{
			NewOptionalField("operation", "int"),
			NewOptionalField("action_index", "long"),
			NewOptionalField("code", "string"),
			NewOptionalField("scope", "string"),
			NewOptionalField("table_name", "string"),
			NewOptionalField("primary_key", "string"),
			NewOptionalField("old_payer", "string"),
			NewOptionalField("new_payer", "string"),
			NewOptionalField("old_data", "bytes"),
			NewOptionalField("new_data", "bytes"),
			NewOptionalField("old_json", jsonData),
			NewOptionalField("new_json", jsonData),
		},
	)
}

type ActionInfoBasic struct {
	Account        string   `json:"account"`
	Receiver       string   `json:"receiver"`
	Name           string   `json:"name"`
	GlobalSequence uint64   `json:"global_seq"`
	Authorization  []string `json:"authorizations"`
}

type ActionInfoBasicSchema = Record

func newActionInfoBasicSchema() ActionInfoBasicSchema {
	return newActionInfoBasicSchemaFQN("ActionInfoBasic", dkafkaNamespace)
}
func newActionInfoBasicSchemaN(name string) ActionInfoBasicSchema {
	return newActionInfoBasicSchemaFQN(name, "")
}
func newActionInfoBasicSchemaFQN(name string, np string) ActionInfoBasicSchema {
	return newRecordFQN(
		np,
		name,
		[]Field{
			{
				Name: "account",
				Type: "string",
			},
			{
				Name: "receiver",
				Type: "string",
			},
			{
				Name: "name",
				Type: "string",
			},
			{
				Name: "global_seq",
				Type: "long",
			},
			{
				Name: "authorizations",
				Type: NewArray("string"),
			},
		},
	)
}

type ActionInfoDetails struct {
	// todo inherit ActionInfoBasic
	Account        string           `json:"account"`
	Receiver       string           `json:"receiver"`
	Action         string           `json:"action"`
	GlobalSequence uint64           `json:"global_seq"`
	Authorization  []string         `json:"authorizations"`
	DBOps          []*decodedDBOp   `json:"db_ops"`
	JSONData       *json.RawMessage `json:"json_data"`
}

func newEventSchema(name string, namespace string, version string, actionInfoSchema Record) Message {
	record := newRecordFQN(
		namespace,
		name,
		[]Field{
			{
				Name: "block_num",
				Type: "long",
			},
			{
				Name: "block_id",
				Type: "string",
			},
			{
				Name: "status",
				Type: "string",
			},
			{
				Name: "executed",
				Type: "boolean",
			},
			{
				Name: "block_step",
				Type: "string",
			},
			NewOptionalField("correlation", newCorrelationRecord()),
			{
				Name: "trx_id",
				Type: "string",
			},
			{
				Name: "act_info",
				Type: actionInfoSchema,
			},
		},
	)
	return Message{
		record,
		Meta{
			Compatibility: "FORWARD",
			Type:          "notification",
			Version:       version,
		},
	}
}

type event struct {
	BlockNum      uint32            `json:"block_num"`
	BlockID       string            `json:"block_id"`
	Status        string            `json:"status"`
	Executed      bool              `json:"executed"`
	Step          string            `json:"block_step"`
	Correlation   *Correlation      `json:"correlation,omitempty"`
	TransactionID string            `json:"trx_id"`
	ActionInfo    ActionInfoDetails `json:"act_info"`
}

func (e event) JSON() []byte {
	b, _ := json.Marshal(e)
	return b

}

type NotificationContext struct {
	BlockNum      uint32       `json:"block_num"`
	BlockID       string       `json:"block_id"`
	Status        string       `json:"status"`
	Executed      bool         `json:"executed"`
	Step          string       `json:"block_step"`
	Correlation   *Correlation `json:"correlation,omitempty"`
	TransactionID string       `json:"trx_id"`
}
type NotificationContextSchema = Record

func newNotificationContextSchema() NotificationContextSchema {
	return newRecordFQN(
		dkafkaNamespace,
		"NotificationContext",
		[]Field{
			{
				Name: "block_num",
				Type: "long",
			},
			{
				Name: "block_id",
				Type: "string",
			},
			{
				Name: "status",
				Type: "string",
			},
			{
				Name: "executed",
				Type: "boolean",
			},
			{
				Name: "block_step",
				Type: "string",
			},
			NewOptionalField("correlation", newCorrelationRecord()),
			{
				Name: "trx_id",
				Type: "string",
			},
		},
	)
}

type TableNotification struct {
	Context NotificationContext `json:"context"`
	Action  ActionInfoBasic     `json:"action"`
	DBOp    *decodedDBOp        `json:"db_op"`
}

func newTableNotificationSchema(name string, namespace string, version string, dbOpRecord Record) Message {
	record := newRecordFQN(
		namespace,
		name,
		[]Field{
			{
				Name: "context",
				Type: newNotificationContextSchema(),
			},
			{
				Name: "action",
				Type: newActionInfoBasicSchema(),
			},
			{
				Name: "db_op",
				Type: dbOpRecord,
			},
		},
	)
	return Message{
		record,
		Meta{
			Compatibility: "FORWARD",
			Type:          "notification",
			Version:       version,
		},
	}
}

type ActionNotification struct {
	Context    NotificationContext `json:"context"`
	ActionInfo ActionInfo          `json:"act_info"`
}

func newActionNotificationSchema(name string, namespace string, version string, actionInfoSchema ActionInfoSchema) Message {
	record := newRecordFQN(
		namespace,
		name,
		[]Field{
			{
				Name: "context",
				Type: newNotificationContextSchema(),
			},
			{
				Name: "act_info",
				Type: actionInfoSchema,
			},
		},
	)
	return Message{
		record,
		Meta{
			Compatibility: "FORWARD",
			Type:          "notification",
			Version:       version,
		},
	}
}

type ActionInfo struct {
	ActionInfoBasic
	JSONData *json.RawMessage `json:"json_data"`
}
type ActionInfoSchema = Record

func newActionInfoSchema(name string, jsonData Record) ActionInfoSchema {
	result := newActionInfoBasicSchemaN(name)
	result.Fields = append(result.Fields, Field{Name: "json_data",
		Type: jsonData,
	})
	return result
}
