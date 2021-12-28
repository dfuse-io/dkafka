package dkafka

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"testing"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
)

func Test_mapper_transform(t *testing.T) {
	tests := []struct {
		file                  string
		failOnUndecodableDBOP bool
		wantErr               bool
	}{
		{
			"testdata/block-30080030.json",
			true,
			false,
		},
		{
			"testdata/block-30080032.json",
			true,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(path.Base(tt.file), func(t *testing.T) {
			f, err := os.Open(tt.file)
			if err != nil {
				t.Fatalf("Open() error: %v", err)
			}
			defer f.Close()
			// read block
			byteValue, err := ioutil.ReadAll(f)
			if err != nil {
				t.Fatalf("ReadAll() error: %v", err)
			}
			block := &pbcodec.Block{}
			// must delete rlimit_ops, valid_block_signing_authority_v2, active_schedule_v2
			err = json.Unmarshal(byteValue, block)
			if err != nil {
				t.Fatalf("Unmarshal() error: %v", err)
			}
			var s sender = &dryRunSender{}
			var localABIFiles = map[string]string{
				"eosio.nft.ft": "testdata/eosio.nft.ft.abi",
			}
			abiFiles, err := LoadABIFiles(localABIFiles)
			if err != nil {
				t.Fatalf("LoadABIFiles() error: %v", err)
			}
			abiDecoder := NewABIDecoder(abiFiles, nil)
			eventTypeProg, err := exprToCelProgram("'TestType'")
			if err != nil {
				t.Fatalf("exprToCelProgram() error: %v", err)
			}
			eventKeyProg, err := exprToCelProgram("[transaction_id]")
			if err != nil {
				t.Fatalf("exprToCelProgram() error: %v", err)
			}
			m := mapper{
				sender:                s,
				topic:                 "test.topic",
				saveBlock:             saveBlockNoop,
				decodeDBOps:           abiDecoder.DecodeDBOps,
				failOnUndecodableDBOP: tt.failOnUndecodableDBOP,
				eventTypeProg:         eventTypeProg,
				eventKeyProg:          eventKeyProg,
				extensions:            nil,
				headers:               nil,
			}

			if err := m.transform(block, "New"); (err != nil) != tt.wantErr {
				t.Errorf("mapper.transform() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
