package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
)

func init() {
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
