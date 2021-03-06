package dkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	pbabicodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/abicodec/v1"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/eoscanada/eos-go"
)

type ABIDecoder struct {
	overrides   map[string]*eos.ABI
	abiCodecCli pbabicodec.DecoderClient
	abisCache   map[string]*abiItem
}

func (a *ABIDecoder) IsNOOP() bool {
	return a.overrides == nil && a.abiCodecCli == nil
}

// LoadABIFiles will load ABIs for different accounts from JSON files
func LoadABIFiles(abiFiles map[string]string) (map[string]*eos.ABI, error) {
	out := make(map[string]*eos.ABI)
	for contract, abiFile := range abiFiles {
		f, err := os.Open(abiFile)
		if err != nil {
			return nil, fmt.Errorf("opening abi file %s: %w", abiFile, err)
		}
		abi, err := eos.NewABI(f)
		if err != nil {
			return nil, fmt.Errorf("reading abi file %s: %w", abiFile, err)
		}
		out[contract] = abi
	}
	return out, nil
}

func NewABIDecoder(
	overrides map[string]*eos.ABI,
	abiCodecCli pbabicodec.DecoderClient,
) *ABIDecoder {
	return &ABIDecoder{
		overrides:   overrides,
		abiCodecCli: abiCodecCli,
		abisCache:   make(map[string]*abiItem),
	}
}

type decodedDBOp struct {
	*pbcodec.DBOp
	NewJSON *json.RawMessage `json:"new_json,omitempty"`
	OldJSON *json.RawMessage `json:"old_json,omitempty"`
}

func (a *ABIDecoder) abi(contract string, blockNum uint32, forceRefresh bool) (*eos.ABI, error) {
	if a.overrides != nil {
		if abi, ok := a.overrides[contract]; ok {
			return abi, nil
		}
	}

	if a.abiCodecCli == nil {
		return nil, fmt.Errorf("unable to get abi for contract %q", contract)
	}

	if !forceRefresh {
		if abiObj, ok := a.abisCache[contract]; ok {
			if abiObj.blockNum < blockNum {
				return abiObj.abi, nil
			}
		}
	}

	resp, err := a.abiCodecCli.GetAbi(context.Background(), &pbabicodec.GetAbiRequest{
		Account:    contract,
		AtBlockNum: blockNum,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to get abi for contract %q: %w", contract, err)
	}

	var abi *eos.ABI
	err = json.Unmarshal([]byte(resp.JsonPayload), &abi)
	if err != nil {
		return nil, fmt.Errorf("unable to decode abi for contract %q: %w", contract, err)
	}

	// store abi in cache for late uses
	a.abisCache[contract] = &abiItem{
		abi:      abi,
		blockNum: resp.AbiBlockNum,
	}
	return abi, nil
}

func (a *ABIDecoder) decodeDBOp(op *decodedDBOp, blockNum uint32, forceRefresh bool) error {
	abi, err := a.abi(op.Code, blockNum, forceRefresh)
	if err != nil {
		return fmt.Errorf("decoding dbop in block %d: %w", blockNum, err)
	}
	tableDef := abi.TableForName(eos.TableName(op.TableName))
	if tableDef == nil {
		return fmt.Errorf("table %s not present in ABI for contract %s", op.TableName, op.Code)
	}

	if len(op.NewData) > 0 {
		bytes, err := abi.DecodeTableRowTyped(tableDef.Type, op.NewData)
		if err != nil {
			return fmt.Errorf("decode row: %w", err)
		}
		asJSON := json.RawMessage(bytes)
		op.NewJSON = &asJSON
	}
	if len(op.OldData) > 0 {
		bytes, err := abi.DecodeTableRowTyped(tableDef.Type, op.OldData)
		if err != nil {
			return fmt.Errorf("decode row: %w", err)
		}
		asJSON := json.RawMessage(bytes)
		op.OldJSON = &asJSON
	}
	return nil
}

func (a *ABIDecoder) DecodeDBOps(in []*pbcodec.DBOp, blockNum uint32) (decodedDBOps []*decodedDBOp, err error) {
	for _, op := range in {
		decoded := &decodedDBOp{DBOp: op}
		decodedDBOps = append(decodedDBOps, decoded)
	}

	if a.IsNOOP() {
		return
	}

	var errors []error
	for _, op := range decodedDBOps {
		err := a.decodeDBOp(op, blockNum, false)
		if err != nil {
			err = a.decodeDBOp(op, blockNum, true) //force refreshing ABI from cache
		}
		if err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		errorStr := ""
		for _, e := range errors {
			errorStr = fmt.Sprintf("%s; %s", errorStr, e.Error())
		}
		err = fmt.Errorf(errorStr)
	}
	return
}

type abiItem struct {
	abi      *eos.ABI
	blockNum uint32
}
