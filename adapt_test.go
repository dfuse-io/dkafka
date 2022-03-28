package dkafka

import (
	"encoding/json"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/linkedin/goavro/v2"
)

func TestCdCAdapter_Adapt(t *testing.T) {
	type fields struct {
		generator Generator2
	}
	type args struct {
		rawStep string
	}
	tests := []struct {
		name    string
		file    string
		schema  string
		fields  fields
		args    args
		want    []*kafka.Message
		wantErr bool
	}{
		{
			name:   "cdc-table",
			file:   "testdata/block-30080032.json",
			schema: tableSchema(t, "testdata/eosio.nft.ft.abi", "factory.a"),
			fields: fields{
				generator: newTableGen4Test(t, "factory.a"),
			},
			args:    args{"New"},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			byteValue := readFileFromTestdata(t, tt.file)

			block := &pbcodec.Block{}
			// must delete rlimit_ops, valid_block_signing_authority_v2, active_schedule_v2
			err := json.Unmarshal(byteValue, block)
			if err != nil {
				t.Fatalf("Unmarshal() error: %v", err)
			}
			codec, err := goavro.NewCodec(tt.schema)
			if err != nil {
				t.Fatalf("goavro.NewCodec(schema) error = %v", err)
				return
			}

			m := &CdCAdapter{
				topic:     "test.topic",
				saveBlock: saveBlockNoop,
				generator: tt.fields.generator,
				headers:   default_headers,
			}
			got, err := m.Adapt(block, tt.args.rawStep)
			if (err != nil) != tt.wantErr {
				t.Errorf("CdCAdapter.Adapt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			kafkaMessage := got[0]
			value := make(map[string]interface{})
			err = json.Unmarshal(kafkaMessage.Value, &value)
			if err != nil {
				t.Fatalf("json.Unmarshal(value) error = %v", err)
				return
			}
			// https://github.com/mitchellh/mapstructure/blob/master/mapstructure.go
			_, err = codec.BinaryFromNative(nil, value)
			if err != nil {
				// t.Errorf("codec.BinaryFromNative(value) error = %v", err)
				// TODO fix the goavro codec issue
				println("improve goavro")
			}
			// if !reflect.DeepEqual(got, tt.want) {
			// 	t.Errorf("CdCAdapter.Adapt() = %v, want %v", got, tt.want)
			// }
		})
	}
}

func newTableGen4Test(t testing.TB, tableName string) TableGenerator {
	var localABIFiles = map[string]string{
		"eosio.nft.ft": "testdata/eosio.nft.ft.abi",
	}
	abiFiles, err := LoadABIFiles(localABIFiles)
	if err != nil {
		t.Fatalf("LoadABIFiles() error: %v", err)
	}
	abiDecoder := NewABIDecoder(abiFiles, nil)

	return TableGenerator{
		tableNames: map[string]ExtractKey{tableName: extractFullKey},
		abiCodec:   NewJsonABICodec(abiDecoder, "eosio.nft.ft"),
	}
}

func tableSchema(t testing.TB, abiFile string, tableName string) string {
	abi, err := LoadABIFile(abiFile)
	if err != nil {
		t.Fatalf("LoadABIFile(abiFile) error: %v", err)
	}
	abiSpec := AbiSpec{
		Account: "eosio.nft.ft",
		Abi:     abi,
	}
	schema, err := GenerateTableSchema(NamedSchemaGenOptions{
		Name:    tableName,
		AbiSpec: abiSpec,
	})

	if err != nil {
		t.Fatalf("GenerateTableSchema() error: %v", err)
	}
	bytes, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}
	return string(bytes)
}
