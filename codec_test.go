package dkafka

import (
	"reflect"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
)

func TestNewJSONCode(t *testing.T) {
	tests := []struct {
		name string
		want Codec
	}{
		{
			"json",
			JSONCodec{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewJSONCodec(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewJSONCode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewKafkaAvroCodec(t *testing.T) {
	schema := newSchema(t, 42, UserSchema, nil)
	type args struct {
		schema *srclient.Schema
	}
	tests := []struct {
		name string
		args args
		want Codec
	}{
		{
			"kafka-avro",
			args{
				schema: schema,
			},
			KafkaAvroCodec{
				RegisteredSchema{
					id:      uint32(schema.ID()),
					schema:  schema.Schema(),
					version: schema.Version(),
					codec:   schema.Codec(),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewKafkaAvroCodec(tt.args.schema); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewKafkaAvroCodec() = %v, want %v", got, tt.want)
			}
		})
	}
}

const UserSchema = `
{
	"namespace": "dkafka.test",
	"name": "User",
	"type": "record",
	"fields": [
		{
			"name": "firstName",
			"type": "string"
		},
		{
			"name": "lastName",
			"type": "string"
		},
		{
			"name": "age",
			"type": "int"
		},
		{
			"name": "id",
			"type": "long"
		}
	]
}
`

func TestCodec_MarshalUnmarshal(t *testing.T) {
	type args struct {
		buf   []byte
		value interface{}
	}
	tests := []struct {
		name             string
		c                Codec
		args             args
		wantMarshalErr   bool
		wantUnmarshalErr bool
	}{
		{
			"json",
			JSONCodec{},
			args{
				nil,
				map[string]interface{}{
					"firstName": "Christophe",
					"lastName":  "O",
					"age":       float64(42),
				},
			},
			false,
			false,
		},
		{
			"kafka-avro",
			NewKafkaAvroCodec(newSchema(t, 42, UserSchema, nil)),
			args{
				nil,
				map[string]interface{}{
					"firstName": "Christophe",
					"lastName":  "O",
					"age":       int32(24),
					"id":        int64(42),
				},
			},
			false,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.c
			gotBytes, err := c.Marshal(tt.args.buf, tt.args.value)
			if (err != nil) != tt.wantMarshalErr {
				t.Errorf("JSONCodec.Marshal() error = %v, wantMarshalErr %v", err, tt.wantMarshalErr)
				return
			}
			gotValue, err := c.Unmarshal(gotBytes)
			if (err != nil) != tt.wantUnmarshalErr {
				t.Errorf("JSONCodec.Unmarshal() error = %v, wantUnmarshalErr %v", err, tt.wantUnmarshalErr)
				return
			}
			if !reflect.DeepEqual(gotValue, tt.args.value) {
				t.Errorf("codec Marshal() then Unmarshal() = %v, want %v", gotValue, tt.args.value)
			}
		})
	}
}

func newSchema(t testing.TB, id uint32, s string, c *goavro.Codec) *srclient.Schema {
	if c == nil {
		c = newAvroCodec(t, s)
	}
	schema, err := srclient.NewSchema(42, UserSchema, srclient.Avro, 1, nil, c, nil)
	if err != nil {
		t.Fatalf("srclient.NewSchema() on schema: %s, error: %v", s, err)
	}
	return schema
}

func newAvroCodec(t testing.TB, s string) *goavro.Codec {
	codec, err := goavro.NewCodec(s)
	if err != nil {
		t.Fatalf("goavro.NewCodec() on schema: %s, error: %v", s, err)
	}
	return codec
}
