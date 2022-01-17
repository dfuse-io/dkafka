package dkafka

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"testing"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
)

func readFileFromTestdata(t testing.TB, file string) []byte {
	f, err := os.Open(file)
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	defer f.Close()
	// read block
	byteValue, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll() error: %v", err)
	}
	return byteValue
}

func Test_adapter_adapt(t *testing.T) {
	tests := []struct {
		file                  string
		expected              string
		failOnUndecodableDBOP bool
		wantErr               bool
	}{
		{
			"testdata/block-30080030.json",
			"",
			true,
			false,
		},
		{
			"testdata/block-30080032.json",
			"testdata/block-30080032-expected.json",
			true,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(path.Base(tt.file), func(t *testing.T) {

			byteValue := readFileFromTestdata(t, tt.file)

			block := &pbcodec.Block{}
			// must delete rlimit_ops, valid_block_signing_authority_v2, active_schedule_v2
			err := json.Unmarshal(byteValue, block)
			if err != nil {
				t.Fatalf("Unmarshal() error: %v", err)
			}
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
			m := adapter{
				topic:                 "test.topic",
				saveBlock:             saveBlockNoop,
				decodeDBOps:           abiDecoder.DecodeDBOps,
				failOnUndecodableDBOP: tt.failOnUndecodableDBOP,
				eventTypeProg:         eventTypeProg,
				eventKeyProg:          eventKeyProg,
				headers:               nil,
			}

			if msg, err := m.adapt(block, "New"); (err != nil) != tt.wantErr {
				t.Errorf("adapter.adapt() error = %v, wantErr %v", err, tt.wantErr)
			} else {
				if tt.expected != "" {
					byteValue := readFileFromTestdata(t, tt.expected)

					var expectedObjectMap map[string]interface{}
					// must delete rlimit_ops, valid_block_signing_authority_v2, active_schedule_v2
					err := json.Unmarshal(byteValue, &expectedObjectMap)
					if err != nil {
						t.Fatalf("Unmarshal() error: %v", err)
					}

					if rawJSON, err := messageToJSON(msg); err != nil {
						t.Errorf("messageToJSON() error: %v", err)
					} else {
						var actualObjectMap map[string]interface{}
						if err := json.Unmarshal(rawJSON, &actualObjectMap); err != nil {
							t.Fatalf("Unmarshal() error: %v", err)
						} else {
							if !reflect.DeepEqual(actualObjectMap, expectedObjectMap) {
								t.Errorf("adapter.adapt() result diff\nactual:\n%s\nexpected:\n%s", string(rawJSON), string(byteValue))
							}
						}
					}
				}
			}
		})
	}
}

func Benchmark_adapter_adapt(b *testing.B) {
	tests := []struct {
		name                  string
		file                  string
		failOnUndecodableDBOP bool
		wantErr               bool
	}{
		{
			"filter-out",
			"testdata/block-30080030.json",
			true,
			false,
		},
		{
			"filter-in",
			"testdata/block-30080032.json",
			true,
			false,
		},
	}
	for _, tt := range tests {
		f, err := os.Open(tt.file)
		if err != nil {
			b.Fatalf("Open() error: %v", err)
		}
		defer f.Close()
		// read block
		byteValue, err := ioutil.ReadAll(f)
		if err != nil {
			b.Fatalf("ReadAll() error: %v", err)
		}
		block := &pbcodec.Block{}
		// must delete rlimit_ops, valid_block_signing_authority_v2, active_schedule_v2
		err = json.Unmarshal(byteValue, block)
		if err != nil {
			b.Fatalf("Unmarshal() error: %v", err)
		}
		var localABIFiles = map[string]string{
			"eosio.nft.ft": "testdata/eosio.nft.ft.abi",
		}
		abiFiles, err := LoadABIFiles(localABIFiles)
		if err != nil {
			b.Fatalf("LoadABIFiles() error: %v", err)
		}
		abiDecoder := NewABIDecoder(abiFiles, nil)
		eventTypeProg, err := exprToCelProgram("'TestType'")
		if err != nil {
			b.Fatalf("exprToCelProgram() error: %v", err)
		}
		eventKeyProg, err := exprToCelProgram("[transaction_id]")
		if err != nil {
			b.Fatalf("exprToCelProgram() error: %v", err)
		}
		m := adapter{
			topic:                 "test.topic",
			saveBlock:             saveBlockNoop,
			decodeDBOps:           abiDecoder.DecodeDBOps,
			failOnUndecodableDBOP: tt.failOnUndecodableDBOP,
			eventTypeProg:         eventTypeProg,
			eventKeyProg:          eventKeyProg,
			headers:               nil,
		}
		b.Run(fmt.Sprintf("%s: %s", tt.name, path.Base(tt.file)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				m.adapt(block, "New")
			}
		})
	}
}
