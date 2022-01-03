package dkafka

import (
	"encoding/json"

	"github.com/google/cel-go/cel"
)

type extension struct {
	name string
	expr string
	prog cel.Program
}

var irreversibleOnly = false

type Correlation struct {
	Payer string `json:"payer"`
	Id    string `json:"id"`
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
