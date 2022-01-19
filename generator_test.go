package dkafka

import (
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"testing"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/google/cel-go/cel"
)

func Test_NewActionGenerator(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		skipKey bool
		want    Generator
		wantErr bool
	}{
		{
			name: "group",
			args: `{"buy":[{"group":["table1", "table2"],"key":"db_ops[0].table_name", "type":"TestEvent"}]}`,
			want: ActionGenerator{
				actions: map[string][]actionHandler{
					"buy": {actionHandler{
						operation: group{
							tables: []string{"table1", "table2"},
						},
						ceType: "TestEvent",
					}},
				},
			},
			skipKey: true,
			wantErr: false,
		},
		{
			name: "filter",
			args: `{"buy":[{"filter":["table1", "table2"],"key":"db_ops[0].table_name", "type":"TestEvent"}]}`,
			want: ActionGenerator{
				actions: map[string][]actionHandler{
					"buy": {actionHandler{
						operation: filter{
							tables: []string{"table1", "table2"},
						},
						ceType: "TestEvent",
					}},
				},
			},
			skipKey: true,
			wantErr: false,
		},
		{
			name: "identity",
			args: `{"buy":[{"key":"db_ops[0].table_name", "type":"TestEvent"}]}`,
			want: ActionGenerator{
				actions: map[string][]actionHandler{
					"buy": {actionHandler{
						operation: indenty{},
						ceType:    "TestEvent",
					}},
				},
			},
			skipKey: true,
			wantErr: false,
		},
		{
			name: "multy-operations",
			args: `{"buy":[{"filter":["table1", "table2"],"key":"db_ops[0].table_name", "type":"TestEvent1"},{"group":["table3"],"key":"db_ops[0].table_name", "type":"TestEvent2"}]}`,
			want: ActionGenerator{
				actions: map[string][]actionHandler{
					"buy": {
						actionHandler{
							operation: filter{
								tables: []string{"table1", "table2"},
							},
							ceType: "TestEvent1",
						},
						actionHandler{
							operation: group{
								tables: []string{"table3"},
							},
							ceType: "TestEvent2",
						},
					},
				},
			},
			skipKey: true,
			wantErr: false,
		},
		{
			name: "multy-actions",
			args: `{"buy":[{"filter":["table1", "table2"],"key":"db_ops[0].table_name", "type":"TestEvent1"}],"issue":[{"group":["table3"],"key":"db_ops[0].table_name", "type":"TestEvent2"}]}`,
			want: ActionGenerator{
				actions: map[string][]actionHandler{
					"buy": {
						actionHandler{
							operation: filter{
								tables: []string{"table1", "table2"},
							},
							ceType: "TestEvent1",
						},
					},
					"issue": {
						actionHandler{
							operation: group{
								tables: []string{"table3"},
							},
							ceType: "TestEvent2",
						},
					},
				},
			},
			skipKey: true,
			wantErr: false,
		},
		{
			name:    "missing-key",
			args:    `{"buy":[{"filter":["table1", "table2"], "type":"TestEvent"}]}`,
			skipKey: false,
			wantErr: true,
		},
		{
			name:    "missing-type",
			args:    `{"buy":[{"filter":["table1", "table2"],"key":"db_ops[0].table_name"}]}`,
			skipKey: false,
			wantErr: true,
		},
		{
			name:    "too-many-operations",
			args:    `{"buy":[{"filter":["table1", "table2"],"group":["table1", "table2"],"key":"db_ops[0].table_name", "type":"TestEvent"}]}`,
			skipKey: false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actionsConf := actionsConfFromJSON(t, tt.args)
			if actionsConf == nil {
				return
			}
			got, err := NewActionsGenerator(actionsConf, tt.skipKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewActionGenerator() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewActionGenerator() = %v, want %v", got, tt.want)
			}
		})
	}
}

func toJSON(v interface{}) json.RawMessage {
	json, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("json.Marshal() error: %v, on: %v", err, v))
	}
	return json
}

func actionsConfFromJSON(t *testing.T, config string) ActionsConf {
	actionsConf := make(ActionsConf)
	err := json.Unmarshal(json.RawMessage(config), &actionsConf)
	if err != nil {
		t.Fatalf("Unmarshal error = %v, config: %s", err, config)
		return nil
	}
	return actionsConf
}

func Test_NewExpressionsGenerator(t *testing.T) {
	t.Run("creation", func(t *testing.T) {
		got := NewExpressionsGenerator(compileExpr("action"), compileExpr("action"))
		_, ok := got.(ExpressionsGenerator)
		if !ok {
			t.Errorf("NewExpressionsGenerator() does not return a ExpressionsGenerator = %v", got)
		}
	})

}

func compileExpr(expr string) cel.Program {
	program, err := exprToCelProgram(expr)
	if err != nil {
		panic(fmt.Sprintf("exprToCelProgram error: %v, expr: %s", err, expr))
	}
	return program
}

func Test_ActionGenerator_Apply(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		config  string
		want    []Generation
		wantErr bool
	}{
		{
			"indentity",
			"testdata/block-30080032.json",
			`{"create":[{"key":"transaction_id", "type":"TestType"}]}`,
			[]Generation{
				{
					Key:          "a2a53dce154c2ccdca52a981318775938de02f7efef88926ae1d7fd992988530",
					CeType:       "TestType",
					DecodedDBOps: jsonToDBOps(DB_OPS_2),
				},
			},
			false,
		},
		{
			"no-action-matching",
			"testdata/block-30080032.json",
			`{"issue":[{"key":"transaction_id", "type":"TestType"}]}`,
			nil,
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
			actionsConfig := actionsConfFromJSON(t, tt.config)
			generator, err := NewActionsGenerator(actionsConfig)
			if err != nil {
				t.Fatalf("NewActionGenerator() error: %v", err)
			}

			trx := block.TransactionTraces()[0]
			act := trx.ActionTraces[0]
			decodedDBOps, err := abiDecoder.DecodeDBOps(trx.DBOpsForAction(act.ExecutionIndex), block.Number)
			if err != nil {
				t.Fatalf("DecodeDBOps() error: %v", err)
			}
			generations, err := generator.Apply("New", trx, act, decodedDBOps)
			if (err != nil) != tt.wantErr {
				t.Errorf("Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			} else if !reflect.DeepEqual(toJSON(generations), toJSON(tt.want)) {
				t.Errorf("\nApply() = \n%s\nWant: \n%s", toJSON(generations), toJSON(tt.want))
			}
		})
	}
}

var DB_OPS_2 string = `
[
	{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 27
		},
		"old_json": {
			"value": 26
		}
	},
	{
		"operation": 1,
		"code": "eosio.nft.ft",
		"scope": "eosio.nft.ft",
		"table_name": "factory.a",
		"primary_key": "...........1e",
		"new_payer": "ultra.nft.ft",
		"new_data": "GgAAAAAAAACQF8hrAnNz1JAXyGsCc3PUAAAAAAAAAAAAAAAAAAAAAAAIVVNEAAAAAAJAKB5JiUMD1PQBQNIeron8Qtz0AQAAAAABAAAAAAABAAAAAAGQF8hrAnNz1AABfGh0dHBzOi8vczMudXMtZWFzdC0xLndhc2FiaXN5cy5jb20vdWx0cmFpby11bmlxLXN0YWdpbmcvZDZjOTk5YmJiZDVmM2MxMjJkMjI2MWUyYjc4N2FjYjNlMDNhYWM2YTE4NTIwMjRiOTY1NmM4NWQ3YTk5ZmFiZC56aXDWyZm7vV88Ei0iYeK3h6yz4DqsahhSAkuWVshdepn6vQEoAAAAAAAAAAAAAAA=",
		"new_json": {
			"asset_creator": "ultra.nft.ft",
			"asset_manager": "ultra.nft.ft",
			"chosen_rate": [],
			"conditionless_receivers": [
				"ultra.nft.ft"
			],
			"conversion_rate_oracle_contract": "",
			"existing_tokens_no": 0,
			"id": 26,
			"lockup_time": 0,
			"max_mintable_tokens": 40,
			"meta_hash": "d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd",
			"meta_uris": [
				"https://s3.us-east-1.wasabisys.com/ultraio-uniq-staging/d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd.zip"
			],
			"minimum_resell_price": "0.00000000 USD",
			"minted_tokens_no": 0,
			"recall_window_start": 0,
			"resale_shares": [
				{
					"basis_point": 500,
					"receiver": "uk1ob2ed3so4"
				},
				{
					"basis_point": 500,
					"receiver": "vl1jt2hi3vd4"
				}
			],
			"stat": 0
		}
	}
]
`

var DB_OPS_2_OP_FIRST string = `
[[
	{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 27
		},
		"old_json": {
			"value": 26
		}
	}
]]
`

var DB_OPS_2_OP_LAST string = `
[[
	{
		"operation": 1,
		"code": "eosio.nft.ft",
		"scope": "eosio.nft.ft",
		"table_name": "factory.a",
		"primary_key": "...........1e",
		"new_payer": "ultra.nft.ft",
		"new_data": "GgAAAAAAAACQF8hrAnNz1JAXyGsCc3PUAAAAAAAAAAAAAAAAAAAAAAAIVVNEAAAAAAJAKB5JiUMD1PQBQNIeron8Qtz0AQAAAAABAAAAAAABAAAAAAGQF8hrAnNz1AABfGh0dHBzOi8vczMudXMtZWFzdC0xLndhc2FiaXN5cy5jb20vdWx0cmFpby11bmlxLXN0YWdpbmcvZDZjOTk5YmJiZDVmM2MxMjJkMjI2MWUyYjc4N2FjYjNlMDNhYWM2YTE4NTIwMjRiOTY1NmM4NWQ3YTk5ZmFiZC56aXDWyZm7vV88Ei0iYeK3h6yz4DqsahhSAkuWVshdepn6vQEoAAAAAAAAAAAAAAA=",
		"new_json": {
			"asset_creator": "ultra.nft.ft",
			"asset_manager": "ultra.nft.ft",
			"chosen_rate": [],
			"conditionless_receivers": [
				"ultra.nft.ft"
			],
			"conversion_rate_oracle_contract": "",
			"existing_tokens_no": 0,
			"id": 26,
			"lockup_time": 0,
			"max_mintable_tokens": 40,
			"meta_hash": "d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd",
			"meta_uris": [
				"https://s3.us-east-1.wasabisys.com/ultraio-uniq-staging/d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd.zip"
			],
			"minimum_resell_price": "0.00000000 USD",
			"minted_tokens_no": 0,
			"recall_window_start": 0,
			"resale_shares": [
				{
					"basis_point": 500,
					"receiver": "uk1ob2ed3so4"
				},
				{
					"basis_point": 500,
					"receiver": "vl1jt2hi3vd4"
				}
			],
			"stat": 0
		}
	}
]]
`

var DB_OPS_4 string = `
[
	{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 27
		},
		"old_json": {
			"value": 26
		}
	},
	{
		"operation": 1,
		"code": "eosio.nft.ft",
		"scope": "eosio.nft.ft",
		"table_name": "factory.a",
		"primary_key": "...........1e",
		"new_payer": "ultra.nft.ft",
		"new_data": "GgAAAAAAAACQF8hrAnNz1JAXyGsCc3PUAAAAAAAAAAAAAAAAAAAAAAAIVVNEAAAAAAJAKB5JiUMD1PQBQNIeron8Qtz0AQAAAAABAAAAAAABAAAAAAGQF8hrAnNz1AABfGh0dHBzOi8vczMudXMtZWFzdC0xLndhc2FiaXN5cy5jb20vdWx0cmFpby11bmlxLXN0YWdpbmcvZDZjOTk5YmJiZDVmM2MxMjJkMjI2MWUyYjc4N2FjYjNlMDNhYWM2YTE4NTIwMjRiOTY1NmM4NWQ3YTk5ZmFiZC56aXDWyZm7vV88Ei0iYeK3h6yz4DqsahhSAkuWVshdepn6vQEoAAAAAAAAAAAAAAA=",
		"new_json": {
			"asset_creator": "ultra.nft.ft",
			"asset_manager": "ultra.nft.ft",
			"chosen_rate": [],
			"conditionless_receivers": [
				"ultra.nft.ft"
			],
			"conversion_rate_oracle_contract": "",
			"existing_tokens_no": 0,
			"id": 26,
			"lockup_time": 0,
			"max_mintable_tokens": 40,
			"meta_hash": "d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd",
			"meta_uris": [
				"https://s3.us-east-1.wasabisys.com/ultraio-uniq-staging/d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd.zip"
			],
			"minimum_resell_price": "0.00000000 USD",
			"minted_tokens_no": 0,
			"recall_window_start": 0,
			"resale_shares": [
				{
					"basis_point": 500,
					"receiver": "uk1ob2ed3so4"
				},
				{
					"basis_point": 500,
					"receiver": "vl1jt2hi3vd4"
				}
			],
			"stat": 0
		}
	},
	{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 28
		},
		"old_json": {
			"value": 27
		}
	},
	{
		"operation": 1,
		"code": "eosio.nft.ft",
		"scope": "eosio.nft.ft",
		"table_name": "factory.a",
		"primary_key": "...........1e",
		"new_payer": "ultra.nft.ft",
		"new_data": "GgAAAAAAAACQF8hrAnNz1JAXyGsCc3PUAAAAAAAAAAAAAAAAAAAAAAAIVVNEAAAAAAJAKB5JiUMD1PQBQNIeron8Qtz0AQAAAAABAAAAAAABAAAAAAGQF8hrAnNz1AABfGh0dHBzOi8vczMudXMtZWFzdC0xLndhc2FiaXN5cy5jb20vdWx0cmFpby11bmlxLXN0YWdpbmcvZDZjOTk5YmJiZDVmM2MxMjJkMjI2MWUyYjc4N2FjYjNlMDNhYWM2YTE4NTIwMjRiOTY1NmM4NWQ3YTk5ZmFiZC56aXDWyZm7vV88Ei0iYeK3h6yz4DqsahhSAkuWVshdepn6vQEoAAAAAAAAAAAAAAA=",
		"new_json": {
			"asset_creator": "ultra.nft.ft",
			"asset_manager": "ultra.nft.ft",
			"chosen_rate": [],
			"conditionless_receivers": [
				"ultra.nft.ft"
			],
			"conversion_rate_oracle_contract": "",
			"existing_tokens_no": 0,
			"id": 27,
			"lockup_time": 0,
			"max_mintable_tokens": 40,
			"meta_hash": "d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd",
			"meta_uris": [
				"https://s3.us-east-1.wasabisys.com/ultraio-uniq-staging/d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd.zip"
			],
			"minimum_resell_price": "0.00000000 USD",
			"minted_tokens_no": 0,
			"recall_window_start": 0,
			"resale_shares": [
				{
					"basis_point": 500,
					"receiver": "uk1ob2ed3so4"
				},
				{
					"basis_point": 500,
					"receiver": "vl1jt2hi3vd4"
				}
			],
			"stat": 0
		}
	}
]
`

var DB_OPS_4_GROUP string = `
[[
	{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 27
		},
		"old_json": {
			"value": 26
		}
	},
	{
		"operation": 1,
		"code": "eosio.nft.ft",
		"scope": "eosio.nft.ft",
		"table_name": "factory.a",
		"primary_key": "...........1e",
		"new_payer": "ultra.nft.ft",
		"new_data": "GgAAAAAAAACQF8hrAnNz1JAXyGsCc3PUAAAAAAAAAAAAAAAAAAAAAAAIVVNEAAAAAAJAKB5JiUMD1PQBQNIeron8Qtz0AQAAAAABAAAAAAABAAAAAAGQF8hrAnNz1AABfGh0dHBzOi8vczMudXMtZWFzdC0xLndhc2FiaXN5cy5jb20vdWx0cmFpby11bmlxLXN0YWdpbmcvZDZjOTk5YmJiZDVmM2MxMjJkMjI2MWUyYjc4N2FjYjNlMDNhYWM2YTE4NTIwMjRiOTY1NmM4NWQ3YTk5ZmFiZC56aXDWyZm7vV88Ei0iYeK3h6yz4DqsahhSAkuWVshdepn6vQEoAAAAAAAAAAAAAAA=",
		"new_json": {
			"asset_creator": "ultra.nft.ft",
			"asset_manager": "ultra.nft.ft",
			"chosen_rate": [],
			"conditionless_receivers": [
				"ultra.nft.ft"
			],
			"conversion_rate_oracle_contract": "",
			"existing_tokens_no": 0,
			"id": 26,
			"lockup_time": 0,
			"max_mintable_tokens": 40,
			"meta_hash": "d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd",
			"meta_uris": [
				"https://s3.us-east-1.wasabisys.com/ultraio-uniq-staging/d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd.zip"
			],
			"minimum_resell_price": "0.00000000 USD",
			"minted_tokens_no": 0,
			"recall_window_start": 0,
			"resale_shares": [
				{
					"basis_point": 500,
					"receiver": "uk1ob2ed3so4"
				},
				{
					"basis_point": 500,
					"receiver": "vl1jt2hi3vd4"
				}
			],
			"stat": 0
		}
	}],
	[{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 28
		},
		"old_json": {
			"value": 27
		}
	},
	{
		"operation": 1,
		"code": "eosio.nft.ft",
		"scope": "eosio.nft.ft",
		"table_name": "factory.a",
		"primary_key": "...........1e",
		"new_payer": "ultra.nft.ft",
		"new_data": "GgAAAAAAAACQF8hrAnNz1JAXyGsCc3PUAAAAAAAAAAAAAAAAAAAAAAAIVVNEAAAAAAJAKB5JiUMD1PQBQNIeron8Qtz0AQAAAAABAAAAAAABAAAAAAGQF8hrAnNz1AABfGh0dHBzOi8vczMudXMtZWFzdC0xLndhc2FiaXN5cy5jb20vdWx0cmFpby11bmlxLXN0YWdpbmcvZDZjOTk5YmJiZDVmM2MxMjJkMjI2MWUyYjc4N2FjYjNlMDNhYWM2YTE4NTIwMjRiOTY1NmM4NWQ3YTk5ZmFiZC56aXDWyZm7vV88Ei0iYeK3h6yz4DqsahhSAkuWVshdepn6vQEoAAAAAAAAAAAAAAA=",
		"new_json": {
			"asset_creator": "ultra.nft.ft",
			"asset_manager": "ultra.nft.ft",
			"chosen_rate": [],
			"conditionless_receivers": [
				"ultra.nft.ft"
			],
			"conversion_rate_oracle_contract": "",
			"existing_tokens_no": 0,
			"id": 27,
			"lockup_time": 0,
			"max_mintable_tokens": 40,
			"meta_hash": "d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd",
			"meta_uris": [
				"https://s3.us-east-1.wasabisys.com/ultraio-uniq-staging/d6c999bbbd5f3c122d2261e2b787acb3e03aac6a1852024b9656c85d7a99fabd.zip"
			],
			"minimum_resell_price": "0.00000000 USD",
			"minted_tokens_no": 0,
			"recall_window_start": 0,
			"resale_shares": [
				{
					"basis_point": 500,
					"receiver": "uk1ob2ed3so4"
				},
				{
					"basis_point": 500,
					"receiver": "vl1jt2hi3vd4"
				}
			],
			"stat": 0
		}
	}
]]
`

var DB_OPS_4_FILTER string = `
[[
	{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 27
		},
		"old_json": {
			"value": 26
		}
	},
	{
		"operation": 2,
		"code": "eosio.nft.ft",
		"table_name": "next.factory",
		"primary_key": "next.factory",
		"old_payer": "eosio.nft.ft",
		"new_payer": "eosio.nft.ft",
		"old_data": "GgAAAAAAAAA=",
		"new_data": "GwAAAAAAAAA=",
		"new_json": {
			"value": 28
		},
		"old_json": {
			"value": 27
		}
	}
]]
`

func Test_operation_on(t *testing.T) {
	type args struct {
		decodedDBOps string
	}
	tests := []struct {
		name string
		op   func() operation
		args args
		want string
	}{
		{
			name: "identity",
			op:   func() operation { return indenty{} },
			args: args{
				decodedDBOps: DB_OPS_2,
			},
			want: "[" + DB_OPS_2 + "]",
		},
		{
			name: "group-first",
			op:   func() operation { return group{[]string{"next.factory"}} },
			args: args{
				decodedDBOps: DB_OPS_2,
			},
			want: DB_OPS_2_OP_FIRST,
		},
		{
			name: "group-last",
			op:   func() operation { return group{[]string{"factory.a"}} },
			args: args{
				decodedDBOps: DB_OPS_2,
			},
			want: DB_OPS_2_OP_LAST,
		},
		{
			name: "group-multi",
			op:   func() operation { return group{[]string{"next.factory", "factory.a"}} },
			args: args{
				decodedDBOps: DB_OPS_4,
			},
			want: DB_OPS_4_GROUP,
		},
		{
			name: "filter-first",
			op:   func() operation { return filter{[]string{"next.factory"}} },
			args: args{
				decodedDBOps: DB_OPS_2,
			},
			want: DB_OPS_2_OP_FIRST,
		},
		{
			name: "filter-last",
			op:   func() operation { return filter{[]string{"factory.a"}} },
			args: args{
				decodedDBOps: DB_OPS_2,
			},
			want: DB_OPS_2_OP_LAST,
		},
		{
			name: "filter-multi",
			op:   func() operation { return filter{[]string{"next.factory"}} },
			args: args{
				decodedDBOps: DB_OPS_4,
			},
			want: DB_OPS_4_FILTER,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := jsonToGroupedDBOps(tt.want)
			if got := tt.op().on(jsonToDBOps(tt.args.decodedDBOps)); !reflect.DeepEqual(got, want) {
				t.Errorf("group.on() = %+v, want %+v", got, want)
			}
		})
	}
}

func jsonToDBOps(dbops string) (result []*decodedDBOp) {
	err := json.Unmarshal(json.RawMessage(dbops), &result)
	if err != nil {
		panic(fmt.Sprintf("Unmarshal() error: %v, on: %s", err, dbops))
	}
	return
}

func jsonToGroupedDBOps(dbops string) (result [][]*decodedDBOp) {
	err := json.Unmarshal(json.RawMessage(dbops), &result)
	if err != nil {
		panic(fmt.Sprintf("Unmarshal() error: %v, on: %s", err, dbops))
	}
	return
}

// func TestExpressionsGenerator_Apply(t *testing.T) {
// 	type fields struct {
// 		keyExtractor    cel.Program
// 		ceTypeExtractor cel.Program
// 	}
// 	type args struct {
// 		stepName     string
// 		transaction  *pbcodec.TransactionTrace
// 		trace        *pbcodec.ActionTrace
// 		decodedDBOps []*decodedDBOp
// 	}
// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		args    args
// 		want    []Generation
// 		wantErr bool
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			g := ExpressionsGenerator{
// 				keyExtractor:    tt.fields.keyExtractor,
// 				ceTypeExtractor: tt.fields.ceTypeExtractor,
// 			}
// 			got, err := g.Apply(tt.args.stepName, tt.args.transaction, tt.args.trace, tt.args.decodedDBOps)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("ExpressionsGenerator.Apply() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("ExpressionsGenerator.Apply() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
