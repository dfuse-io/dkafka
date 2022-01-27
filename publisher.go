package dkafka

import (
	"encoding/json"
)

var irreversibleOnly = false

func newCorrelationRecord() Record {
	return newRecordS(
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

func newActionInfoSchema(name string, jsonData Record, dbOpsRecord Record) Record {
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
			{
				Name: "operation",
				Type: NewOptional("int"),
			},
			{
				Name: "action_index",
				Type: NewOptional("long"),
			},
			{
				Name: "code",
				Type: NewOptional("string"),
			},
			{
				Name: "scope",
				Type: NewOptional("string"),
			},
			{
				Name: "table_name",
				Type: NewOptional("string"),
			},
			{
				Name: "primary_key",
				Type: NewOptional("string"),
			},
			{
				Name: "old_payer",
				Type: NewOptional("string"),
			},
			{
				Name: "new_payer",
				Type: NewOptional("string"),
			},
			{
				Name: "old_data",
				Type: NewOptional("bytes"),
			},
			{
				Name: "new_data",
				Type: NewOptional("bytes"),
			},
			{
				Name: "old_json",
				Type: NewOptional(jsonData),
			},
			{
				Name: "new_json",
				Type: NewOptional(jsonData),
			},
		},
	)
}

type ActionInfo struct {
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
			{
				Name: "correlation",
				Type: NewOptional(newCorrelationRecord()),
			},
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
	BlockNum      uint32       `json:"block_num"`
	BlockID       string       `json:"block_id"`
	Status        string       `json:"status"`
	Executed      bool         `json:"executed"`
	Step          string       `json:"block_step"`
	Correlation   *Correlation `json:"correlation,omitempty"`
	TransactionID string       `json:"trx_id"`
	ActionInfo    ActionInfo   `json:"act_info"`
}

func (e event) JSON() []byte {
	b, _ := json.Marshal(e)
	return b

}

type TableEvent struct {
	BlockNum       uint32       `json:"block_num"`
	BlockID        string       `json:"block_id"`
	Status         string       `json:"status"`
	Executed       bool         `json:"executed"`
	Step           string       `json:"block_step"`
	Correlation    *Correlation `json:"correlation,omitempty"`
	TransactionID  string       `json:"trx_id"`
	Action         string       `json:"action"`
	GlobalSequence uint64       `json:"global_seq"`
	DBOp           *decodedDBOp `json:"db_op"`
}

func newTableNotificationSchema(name string, namespace string, version string, dbOpRecord Record) Message {
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
			{
				Name: "correlation",
				Type: NewOptional(newCorrelationRecord()),
			},
			{
				Name: "trx_id",
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

type ActionEvent struct {
	BlockNum      uint32       `json:"block_num"`
	BlockID       string       `json:"block_id"`
	Status        string       `json:"status"`
	Executed      bool         `json:"executed"`
	Step          string       `json:"block_step"`
	Correlation   *Correlation `json:"correlation,omitempty"`
	TransactionID string       `json:"trx_id"`
	ActionInfo    ActionInfo2  `json:"act_info"`
}

type ActionInfo2 struct {
	Account        string           `json:"account"`
	Receiver       string           `json:"receiver"`
	Action         string           `json:"action"`
	GlobalSequence uint64           `json:"global_seq"`
	Authorization  []string         `json:"authorizations"`
	JSONData       *json.RawMessage `json:"json_data"`
}

type ActionInfo2Record = Record

func newActionNotificationSchema(name string, namespace string, version string, actionInfoSchema ActionInfo2Record) Message {
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
			{
				Name: "correlation",
				Type: NewOptional(newCorrelationRecord()),
			},
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

func newActionInfo2Schema(name string, jsonData Record) ActionInfo2Record {
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
				Name: "json_data",
				Type: jsonData,
			},
		},
	)
}
