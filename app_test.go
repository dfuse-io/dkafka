package dkafka

import (
	"reflect"
	"testing"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
)

func Test_getCompressionLevel(t *testing.T) {
	type args struct {
		compressionType string
		config          *Config
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{"default", args{"snappy", &Config{KafkaCompressionLevel: -1}}, -1},
		{"gzip normal", args{"gzip", &Config{KafkaCompressionLevel: 2}}, 2},
		{"gzip outer up", args{"gzip", &Config{KafkaCompressionLevel: 12}}, 9},
		{"gzip outer down", args{"gzip", &Config{KafkaCompressionLevel: -2}}, 0},
		{"snappy normal", args{"snappy", &Config{KafkaCompressionLevel: 0}}, 0},
		{"snappy outer up", args{"snappy", &Config{KafkaCompressionLevel: 12}}, 0},
		{"snappy outer down", args{"snappy", &Config{KafkaCompressionLevel: -2}}, 0},
		{"lz4 normal", args{"lz4", &Config{KafkaCompressionLevel: 12}}, 12},
		{"lz4 outer up", args{"lz4", &Config{KafkaCompressionLevel: 143}}, 12},
		{"lz4 outer down", args{"lz4", &Config{KafkaCompressionLevel: -12}}, 0},
		{"zstd normal", args{"zstd", &Config{KafkaCompressionLevel: 2}}, -1},
		{"zstd outer up", args{"zstd", &Config{KafkaCompressionLevel: 12}}, -1},
		{"zstd outer down", args{"zstd", &Config{KafkaCompressionLevel: -2}}, -1},
		{"unknown", args{"????", &Config{KafkaCompressionLevel: 4}}, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getCompressionLevel(tt.args.compressionType, tt.args.config); got != tt.want {
				t.Errorf("getCompressionLevel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getCorrelation(t *testing.T) {
	type args struct {
		Actions []*pbcodec.ActionTrace
	}
	tests := []struct {
		Name            string
		Args            args
		WantCorrelation *Correlation
		WantErr         bool
	}{
		{"empty", args{}, nil, false},
		{"not-empty-no-correlations", args{[]*pbcodec.ActionTrace{{Action: &pbcodec.Action{Account: "eosio.token", Name: "transfer"}}}}, nil, false},
		{"not-empty-with-correlations-first", args{[]*pbcodec.ActionTrace{{Action: &pbcodec.Action{Account: "ultra.tools", Name: "correlate", JsonData: `{"payer":"ultra", "correlation_id":"123"}`}}, {Action: &pbcodec.Action{Account: "eosio.token", Name: "transfer"}}}}, &Correlation{"ultra", "123"}, false},
		{"missing-correlations-json-data", args{[]*pbcodec.ActionTrace{{Action: &pbcodec.Action{Account: "ultra.tools", Name: "correlate"}}}}, nil, true},
		{"not-empty-with-correlations-second", args{[]*pbcodec.ActionTrace{{Action: &pbcodec.Action{Account: "eosio.token", Name: "transfer"}}, {Action: &pbcodec.Action{Account: "ultra.tools", Name: "correlate", JsonData: `{"payer":"ultra", "correlation_id":"123"}`}}}}, &Correlation{"ultra", "123"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			gotCorrelation, err := getCorrelation(tt.Args.Actions)
			if (err != nil) != tt.WantErr {
				t.Errorf("getCorrelation() error = %v, wantErr %v", err, tt.WantErr)
				return
			}
			if !reflect.DeepEqual(gotCorrelation, tt.WantCorrelation) {
				t.Errorf("getCorrelation() = %v, want %v", gotCorrelation, tt.WantCorrelation)
			}
		})
	}
}
