package dkafka

import (
	"fmt"
	"strconv"
	"strings"

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
	First  string   `json:"first,omitempty"`
	Key    string   `json:"key"`
	CeType string   `json:"type"`
}

type ActionsConf = map[string][]ActionConf

func NewActionsGenerator(config ActionsConf, skipKey ...bool) (Generator, error) {
	handlersByAction := make(map[string][]actionHandler)
	for actionName, actions := range config {
		actionHandlers := make([]actionHandler, 0, 1)
		for _, action := range actions {
			if action.Group != nil && action.Filter != nil {
				return nil, fmt.Errorf("only one operation at a time per action handler is supported. Action:%s", actionName)
			}
			var operation operation
			if action.Group != nil {
				matchers, err := expressionsToMatchers(action.Group)
				if err != nil {
					return nil, fmt.Errorf("invalid group expression: %v, error: %w", action.Group, err)
				}
				operation = group{
					matchers: matchers,
				}
			} else if action.Filter != nil {
				matchers, err := expressionsToMatchers(action.Filter)
				if err != nil {
					return nil, fmt.Errorf("invalid filter expression: %v, error: %w", action.Filter, err)
				}
				operation = filter{
					matchers: matchers,
				}
			} else if action.First != "" {
				matcher, err := expressionToMatcher(action.First)
				if err != nil {
					return nil, fmt.Errorf("invalid filter expression: %v, error: %w", action.First, err)
				}
				operation = first{
					table: matcher,
				}
			} else {
				// indentity dbops operation
				operation = indentity{}
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

func expressionsToMatchers(expressions []string) ([]matcher, error) {
	matchers := make([]matcher, len(expressions))
	for i, expression := range expressions {
		matcher, err := expressionToMatcher(expression)
		if err != nil {
			return nil, err
		}
		matchers[i] = matcher
	}
	return matchers, nil
}

func expressionToMatcher(expression string) (matcher, error) {
	tokens := strings.Split(expression, ":")
	if len(tokens) == 1 {
		return tableNameMatcher{tokens[0]}, nil
	} else if tokens[0] == "*" && tokens[1] == "*" {
		return nil, fmt.Errorf("unsupported table matcher expression: %s, you must at least fix on of them", expression)
	} else if tokens[0] == "*" {
		return tableNameMatcher{tokens[1]}, nil
	} else if tokens[1] == "*" {
		op, err := strconv.Atoi(tokens[0])
		if err != nil || op < 0 || op > 3 {
			return nil, fmt.Errorf("invalid operation matcher value must be a uint between [0..3], on: %s, with error: %w", expression, err)
		}
		return operationMatcher{pbcodec.DBOp_Operation(op)}, nil
	} else {
		op, err := strconv.Atoi(tokens[0])
		if err != nil || op < 0 || op > 3 {
			return nil, fmt.Errorf("invalid operation matcher value must be a uint between [0..3]")
		}
		return operationOnTableMatcher{op: pbcodec.DBOp_Operation(op), name: tokens[1]}, nil
	}
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

type first struct {
	table matcher
}

func (f first) on(decodedDBOps []*decodedDBOp) [][]*decodedDBOp {
	for _, dbOp := range decodedDBOps {
		if f.table.match(dbOp.DBOp) {
			return [][]*decodedDBOp{{dbOp}}
		}
	}
	return nil
}

type filter struct {
	matchers []matcher
}

func (f filter) on(decodedDBOps []*decodedDBOp) [][]*decodedDBOp {
	var filteredDBOps []*decodedDBOp = make([]*decodedDBOp, 0, 1)
	for _, dbOp := range decodedDBOps {
		for _, matcher := range f.matchers {
			if matcher.match(dbOp.DBOp) {
				filteredDBOps = append(filteredDBOps, dbOp)
			}
		}
	}
	return [][]*decodedDBOp{filteredDBOps}
}

type matcher interface {
	match(value *pbcodec.DBOp) bool
}

type tableNameMatcher struct {
	name string
}

func (m tableNameMatcher) match(value *pbcodec.DBOp) bool {
	return m.name == value.TableName
}

type operationMatcher struct {
	op pbcodec.DBOp_Operation
}

func (m operationMatcher) match(value *pbcodec.DBOp) bool {
	return m.op == value.Operation
}

type operationOnTableMatcher struct {
	name string
	op   pbcodec.DBOp_Operation
}

func (m operationOnTableMatcher) match(value *pbcodec.DBOp) bool {
	return m.op == value.Operation && m.name == value.TableName
}

type indentity struct {
}

func (i indentity) on(decodedDBOps []*decodedDBOp) [][]*decodedDBOp {
	return [][]*decodedDBOp{decodedDBOps}
}

type group struct {
	matchers []matcher
}

func (g group) on(decodedDBOps []*decodedDBOp) [][]*decodedDBOp {
	groups := make([][]*decodedDBOp, 0, 1)
	groupSize := len(g.matchers)
	groupCursor := 0
	group := make([]*decodedDBOp, 0, 1)
	for _, dbOp := range decodedDBOps {
		if g.matchers[groupCursor].match(dbOp.DBOp) {
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
