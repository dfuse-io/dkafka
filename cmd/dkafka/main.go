package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"

	"go.uber.org/zap"
)

func init() {
}

func main() {
	go func() {
		zlog.Debug("starting pprof logging", zap.Error(http.ListenAndServe("localhost:6060", nil)))
	}()

	if err := RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
