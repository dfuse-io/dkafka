package dkafka

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"

	"github.com/google/cel-go/cel"
)

type dfuseConnectionConfig struct {
	DfuseGRPCEndpoint string
	DfuseToken        string
}

type publisherKafkaConnectionConfig struct {
	KafkaEndpoints         []string
	KafkaSSLEnable         bool
	KafkaSSLCAFile         string
	KafkaSSLInsecure       bool
	KafkaSSLAuth           bool
	KafkaSSLClientCertFile string
	KafkaSSLClientKeyFile  string
}

type publisherJobConfig struct {
	IncludeFilterExpr string
	KafkaTopic        string
	EventSource       string
	EventKeysExpr     string
	EventTypeExpr     string
	EventExtensions   map[string]string
}

type extension struct {
	name string
	expr string
	prog cel.Program
}

var irreversibleOnly = false

type ActionInfo struct {
	Account        string           `json:"account"`
	Receiver       string           `json:"receiver"`
	Action         string           `json:"action"`
	GlobalSequence uint64           `json:"global_seq"`
	Authorization  []string         `json:"authorizations"`
	DBOps          []*pbcodec.DBOp  `json:"db_ops"`
	JSONData       *json.RawMessage `json:"json_data"`
}

type event struct {
	BlockNum      uint32     `json:"block_num"`
	BlockID       string     `json:"block_id"`
	Status        string     `json:"status"`
	Step          string     `json:"block_step"`
	TransactionID string     `json:"trx_id"`
	ActionInfo    ActionInfo `json:"act_info"`
}

func (e event) JSON() []byte {
	b, _ := json.Marshal(e)
	return b

}

func hashString(data string) []byte {
	h := sha256.New()
	h.Write([]byte(data))
	return []byte(base64.StdEncoding.EncodeToString(([]byte(h.Sum(nil)))))
}

var stringType = reflect.TypeOf("")
var stringArrayType = reflect.TypeOf([]string{})

func evalString(prog cel.Program, activation interface{}) (string, error) {
	res, _, err := prog.Eval(activation)
	if err != nil {
		return "", err
	}
	out, err := res.ConvertToNative(stringType)
	if err != nil {
		return "", err
	}
	return out.(string), nil
}

func evalStringArray(prog cel.Program, activation interface{}) ([]string, error) {
	res, _, err := prog.Eval(activation)
	if err != nil {
		return nil, err
	}
	out, err := res.ConvertToNative(stringArrayType)
	if err != nil {
		return nil, err
	}
	return out.([]string), nil
}

func sanitizeStep(step string) string {
	return strings.Title(strings.TrimPrefix(step, "STEP_"))
}
func sanitizeStatus(status string) string {
	return strings.Title(strings.TrimPrefix(status, "TRANSACTIONSTATUS_"))
}
