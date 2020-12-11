package dkafka

import (
	"fmt"

	"github.com/dfuse-io/dfuse-eosio/filtering"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
)

func exprToCelProgram(stripped string) (prog cel.Program, err error) {
	env, err := cel.NewEnv(filtering.ActionTraceDeclarations)
	if err != nil {
		fmt.Println(err)
		return
	}

	exprAst, issues := env.Compile(stripped)
	if issues != nil && issues.Err() != nil {
		fmt.Println(fmt.Errorf("parse filter: %w", issues.Err()))
		return
	}
	fmt.Println(exprAst.ResultType().GetType())
	fmt.Println(decls.NewListType(decls.String).GetType())

	prog, err = env.Program(exprAst)
	if err != nil {
		fmt.Println(fmt.Errorf("program: %w", err))
		return
	}

	return
}
