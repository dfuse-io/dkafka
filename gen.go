package dkafka

import (
	"encoding/json"
	"fmt"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/google/cel-go/cel"
)

type GenContext struct {
	block       *pbcodec.Block
	stepName    string
	transaction *pbcodec.TransactionTrace
	actionTrace *pbcodec.ActionTrace
	correlation *Correlation
}

type Generator2 interface {
	Apply(genContext GenContext) ([]Generation2, error)
}

type Generation2 struct {
	CeType string      `json:"ce_type,omitempty"`
	CeId   []byte      `json:"ce_id,omitempty"`
	Key    string      `json:"key,omitempty"`
	Value  interface{} `json:"value,omitempty"`
}

type DecodeDBOp = func(in *pbcodec.DBOp, blockNum uint32) (decodedDBOps *decodedDBOp, err error)

type TableGenerator struct {
	decodeDBOp DecodeDBOp
	tableNames StringSet
}

type void struct{}
type StringSet = map[string]void

var empty = void{}

func (tg TableGenerator) Apply(gc GenContext) ([]Generation2, error) {
	dbOps := gc.transaction.DBOpsForAction(gc.actionTrace.ExecutionIndex)
	generations := []Generation2{}
	for _, dbOp := range dbOps {
		if dbOp.Operation == pbcodec.DBOp_OPERATION_UNKNOWN {
			continue
		}
		// extractor, found := tg.tableNames[dbOp.TableName]
		_, found := tg.tableNames[dbOp.TableName]
		if !found {
			continue
		}
		decodedDBOp, err := tg.decodeDBOp(dbOp, gc.block.Number)
		if err != nil {
			return nil, err
		}
		// activation, err := NewTableActivation(
		// 	gc.stepName,
		// 	gc.transaction,
		// 	gc.actionTrace,
		// 	decodedDBOp,
		// )
		// if err != nil {
		// 	return nil, err
		// }
		// key, err := evalString(extractor, activation)
		// if err != nil {
		// 	return nil, err
		// }
		key := fmt.Sprintf("%s:%s", decodedDBOp.Scope, decodedDBOp.PrimaryKey)
		_, ceType := tableCeType(dbOp.TableName)
		ceId := hashString(fmt.Sprintf(
			"%s%s%d%d%s%s%s",
			gc.block.Id,
			gc.transaction.Id,
			gc.actionTrace.ExecutionIndex,
			dbOp.Operation,
			dbOp.TableName,
			gc.stepName,
			key))
		value := TableNotification{
			Context: notificationContext(gc),
			Action:  actionInfoBasic(gc),
			DBOp:    decodedDBOp,
		}
		generations = append(generations, Generation2{
			CeId:   ceId,
			CeType: ceType,
			Key:    key,
			Value:  value,
		})
	}
	return generations, nil
}

type ActionGenerator2 struct {
	keyExtractors map[string]cel.Program
}

func (ag ActionGenerator2) Apply(gc GenContext) ([]Generation2, error) {
	actionName := gc.actionTrace.Action.Name
	extractor, found := ag.keyExtractors[actionName]
	if !found {
		return []Generation2{}, nil
	}
	activation, err := NewActionActivation(
		gc.stepName,
		gc.transaction,
		gc.actionTrace,
	)
	if err != nil {
		return nil, err
	}
	// TODO see if we need a defaulting on the key
	// key := fmt.Sprintf("%s:%d", gc.transaction.Id, gc.actionTrace.ExecutionIndex)
	key, err := evalString(extractor, activation)
	if err != nil {
		return nil, err
	}
	_, ceType := actionCeType(actionName)
	ceId := hashString(fmt.Sprintf(
		"%s%s%d%s%s%s",
		gc.block.Id,
		gc.transaction.Id,
		gc.actionTrace.ExecutionIndex,
		ceType,
		gc.stepName,
		key))
	var jsonData json.RawMessage
	if stringData := gc.actionTrace.Action.JsonData; stringData != "" {
		jsonData = json.RawMessage(stringData)
	}
	value := ActionNotification{
		Context: notificationContext(gc),
		ActionInfo: ActionInfo{
			ActionInfoBasic: actionInfoBasic(gc),
			JSONData:        &jsonData,
		},
	}

	return []Generation2{{
		CeId:   ceId,
		CeType: ceType,
		Key:    key,
		Value:  value,
	}}, nil
}

func notificationContext(gc GenContext) NotificationContext {
	status := sanitizeStatus(gc.transaction.Receipt.Status.String())

	return NotificationContext{
		BlockNum:      gc.block.Number,
		BlockID:       gc.block.Id,
		Status:        status,
		Executed:      !gc.transaction.HasBeenReverted(),
		Step:          gc.stepName,
		Correlation:   gc.correlation,
		TransactionID: gc.transaction.Id,
	}
}

func actionInfoBasic(gc GenContext) ActionInfoBasic {
	var globalSeq uint64
	if receipt := gc.actionTrace.Receipt; receipt != nil {
		globalSeq = receipt.GlobalSequence
	}

	var authorizations []string
	for _, auth := range gc.actionTrace.Action.Authorization {
		authorizations = append(authorizations, auth.Authorization())
	}

	return ActionInfoBasic{
		Account:        gc.actionTrace.Account(),
		Receiver:       gc.actionTrace.Receiver,
		Name:           gc.actionTrace.Name(),
		GlobalSequence: globalSeq,
		Authorization:  authorizations,
	}
}
