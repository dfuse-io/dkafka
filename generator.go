package dkafka

import (
	"fmt"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/google/cel-go/cel"
)

type Generation struct {
	Key          string         `json:"key,omitempty"`
	CeType       string         `json:"type,omitempty"`
	DecodedDBOps []*decodedDBOp `json:"db_ops,omitempty"`
}

type Generator interface {
	Apply(
		stepName string,
		transaction *pbcodec.TransactionTrace,
		trace *pbcodec.ActionTrace,
		decodedDBOps []*decodedDBOp,
	) ([]Generation, error)
}

type ActionConf struct {
	Group  []string `json:"group,omitempty"`
	Filter []string `json:"filter,omitempty"`
	Key    string   `json:"key"`
	CeType string   `json:"type"`
}

type ActionsConf = map[string][]ActionConf

func NewActionGenerator(config ActionsConf, skipKey ...bool) (Generator, error) {
	handlersByAction := make(map[string][]actionHandler)
	for actionName, actions := range config {
		actionHandlers := make([]actionHandler, 0, 1)
		for _, action := range actions {
			if action.Group != nil && action.Filter != nil {
				return nil, fmt.Errorf("only one operation at a time per action handler is supported. Action:%s", actionName)
			}
			var operation operation
			if action.Group != nil {
				operation = group{
					tables: action.Group,
				}
			} else if action.Filter != nil {
				operation = filter{
					tables: action.Filter,
				}
			} else {
				// indenty dbops operation
				operation = indenty{}
			}
			skipK := (len(skipKey) > 0 && skipKey[0])
			var keyExtractor cel.Program
			if action.Key != "" && !skipK {
				var err error
				keyExtractor, err = exprToCelProgram(action.Key)
				if err != nil {
					return nil, fmt.Errorf("action: %s, cannot parse key expression: %w", actionName, err)
				}
			} else if !skipK && action.Key == "" {
				return nil, fmt.Errorf("missing key expression for action: %s", actionName)
			}
			if action.CeType == "" {
				return nil, fmt.Errorf("missing type string for action: %s", actionName)
			}
			actionHandlers = append(actionHandlers, actionHandler{
				operation:    operation,
				ceType:       action.CeType,
				keyExtractor: keyExtractor,
			})
		}

		handlersByAction[actionName] = actionHandlers
	}
	return ActionGenerator{
		actions: handlersByAction,
	}, nil
}

func NewExpressionsGenerator(
	eventKeyProg cel.Program,
	eventTypeProg cel.Program,
) Generator {
	return ExpressionsGenerator{
		keyExtractor:    eventKeyProg,
		ceTypeExtractor: eventTypeProg,
	}
}

type operation interface {
	on(decodedDBOps []*decodedDBOp) [][]*decodedDBOp
}

type filter struct {
	tables []string
}

func (f filter) on(decodedDBOps []*decodedDBOp) [][]*decodedDBOp {
	var filteredDBOps []*decodedDBOp = make([]*decodedDBOp, 0, 1)
	for _, dbOp := range decodedDBOps {
		for _, table := range f.tables {
			if dbOp.TableName == table {
				filteredDBOps = append(filteredDBOps, dbOp)
			}
		}
	}
	return [][]*decodedDBOp{filteredDBOps}
}

type indenty struct {
}

func (i indenty) on(decodedDBOps []*decodedDBOp) [][]*decodedDBOp {
	return [][]*decodedDBOp{decodedDBOps}
}

type group struct {
	tables []string
}

func (g group) on(decodedDBOps []*decodedDBOp) [][]*decodedDBOp {
	groups := make([][]*decodedDBOp, 0, 1)
	groupSize := len(g.tables)
	groupCursor := 0
	group := make([]*decodedDBOp, 0, 1)
	for _, dbOp := range decodedDBOps {
		if dbOp.TableName == g.tables[groupCursor] {
			group = append(group, dbOp)
			groupCursor++
		}
		if groupCursor == groupSize {
			groupCursor = 0
			groups = append(groups, group)
			group = make([]*decodedDBOp, 0, 1)
		}
	}
	if groupCursor > 0 {
		groups = append(groups, group)
	}
	return groups
}

type actionHandler struct {
	operation    operation
	keyExtractor cel.Program
	ceType       string
}

type ActionGenerator struct {
	actions map[string][]actionHandler
}

func (g ActionGenerator) Apply(stepName string,
	transaction *pbcodec.TransactionTrace,
	trace *pbcodec.ActionTrace,
	decodedDBOps []*decodedDBOp,
) ([]Generation, error) {
	if actionHandlers, ok := g.actions[trace.Action.Name]; ok {
		var generations []Generation = make([]Generation, 0, 1)
		for i, actionHandler := range actionHandlers {
			for _, ops := range actionHandler.operation.on(decodedDBOps) {

				activation, err := NewActivation(stepName, transaction,
					trace,
					ops,
				)
				if err != nil {
					return nil, err
				}
				ceType := actionHandler.ceType

				key, err := evalString(actionHandler.keyExtractor, activation)
				if err != nil {
					return nil, fmt.Errorf("action:%s, index:%d event keyeval: %w", trace.Action.Name, i, err)
				}
				generations = append(generations, Generation{
					key,
					ceType,
					ops,
				})
			}

		}
		return generations, nil
	}
	return nil, nil
}

type ExpressionsGenerator struct {
	keyExtractor    cel.Program
	ceTypeExtractor cel.Program
}

func (g ExpressionsGenerator) Apply(stepName string,
	transaction *pbcodec.TransactionTrace,
	trace *pbcodec.ActionTrace,
	decodedDBOps []*decodedDBOp,
) ([]Generation, error) {
	activation, err := NewActivation(stepName, transaction,
		trace,
		decodedDBOps,
	)
	if err != nil {
		return nil, err
	}

	eventType, err := evalString(g.ceTypeExtractor, activation)
	if err != nil {
		return nil, fmt.Errorf("error eventtype eval: %w", err)
	}

	eventKeys, err := evalStringArray(g.keyExtractor, activation)
	if err != nil {
		return nil, fmt.Errorf("event keyeval: %w", err)
	}
	var generations []Generation = make([]Generation, 0, 1)
	dedupeMap := make(map[string]bool)
	for _, eventKey := range eventKeys {
		if dedupeMap[eventKey] {
			continue
		}
		dedupeMap[eventKey] = true
		generations = append(generations, Generation{
			eventKey,
			eventType,
			decodedDBOps,
		})
	}
	return generations, nil
}
