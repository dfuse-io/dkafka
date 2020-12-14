package dkafka

import (
	"fmt"

	"github.com/dfuse-io/dfuse-eosio/filtering"
	"github.com/google/cel-go/cel"
)

func exprToCelProgram(stripped string) (prog cel.Program, err error) {
	env, err := cel.NewEnv(filtering.ActionTraceDeclarations)
	if err != nil {
		return nil, fmt.Errorf("creating new CEL environment: %w", err)
	}

	exprAst, issues := env.Compile(stripped)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("compiling AST expression %s: %w", stripped, issues.Err())
	}

	prog, err = env.Program(exprAst)
	if err != nil {
		return nil, fmt.Errorf("creating program from AST expression %s: %w", stripped, err)
	}

	return
}
