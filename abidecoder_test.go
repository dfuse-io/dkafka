package dkafka

import (
	"fmt"
	"testing"

	"github.com/eoscanada/eos-go"
	"github.com/riferrei/srclient"
)

func TestABIDecoderOnReload(t *testing.T) {
	abiCodec := ABIDecoder{
		onReload: func() {},
	}
	abiCodec.onReload()
}

func TestKafkaAvroABICodec_GetCodec(t *testing.T) {
	type args struct {
		name     string
		blockNum uint32
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "static",
			args: args{
				name:     dkafkaCheckpoint,
				blockNum: 0,
			},
			wantErr: false,
		},
	}
	var localABIFiles = map[string]string{
		"eosio.nft.ft": "testdata/eosio.nft.ft.abi",
	}
	abiFiles, err := LoadABIFiles(localABIFiles)
	if err != nil {
		t.Fatalf("LoadABIFiles() error: %v", err)
	}
	abiDecoder := NewABIDecoder(abiFiles, nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewKafkaAvroABICodec(
				abiDecoder,
				func(s string, a *eos.ABI) (MessageSchema, error) { return MessageSchema{}, fmt.Errorf("unsupported") },
				srclient.CreateMockSchemaRegistryClient("mock://TestKafkaAvroABICodec_GetCodec"),
				"eosio.nft.ft",
				"mock://TestKafkaAvroABICodec_GetCodec",
			)
			got, err := c.GetCodec(tt.args.name, tt.args.blockNum)
			if (err != nil) != tt.wantErr {
				t.Errorf("KafkaAvroABICodec.GetCodec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Errorf("KafkaAvroABICodec.GetCodec() = %v, want not nil", got)
			}
		})
	}
}
