package dkafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
)

type CodecType = string

const (
	AvroCodec CodecType = "avro"
	JsonCodec CodecType = "json"
)

type Codec interface {
	Marshal(buf []byte, value interface{}) ([]byte, error)
	Unmarshal(buf []byte) (interface{}, error)
}

func NewJSONCodec() Codec {
	return JSONCodec{}
}

type JSONCodec struct{}

func (c JSONCodec) Marshal(buf []byte, value interface{}) (bytes []byte, err error) {
	bytes, err = json.Marshal(value)
	if err != nil {
		bytes = buf
	} else {
		bytes = append(buf, bytes...)
	}
	return
}

func (c JSONCodec) Unmarshal(buf []byte) (interface{}, error) {
	value := make(map[string]interface{})
	err := json.Unmarshal(buf, &value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func NewKafkaAvroCodec(schema *srclient.Schema) Codec {
	return KafkaAvroCodec{
		schema: RegisteredSchema{
			id:      uint32(schema.ID()),
			schema:  schema.Schema(),
			version: schema.Version(),
			codec:   schema.Codec(),
		},
	}
}

type RegisteredSchema struct {
	id      uint32
	schema  string
	version int
	codec   *goavro.Codec
}

type KafkaAvroCodec struct {
	schema RegisteredSchema
}

func (c KafkaAvroCodec) Marshal(buf []byte, value interface{}) (bytes []byte, err error) {
	schemaHeaderBytes := make([]byte, 5)
	// append magic byte 0
	// append schema id in int32 bigendian
	binary.BigEndian.PutUint32(schemaHeaderBytes[1:5], uint32(c.schema.id))
	bytes = append(bytes, schemaHeaderBytes...)
	// append append value
	bytes, err = c.schema.codec.BinaryFromNative(bytes, value)
	if err != nil {
		bytes = buf
	} else {
		bytes = append(buf, bytes...)
	}
	return
}

func (c KafkaAvroCodec) Unmarshal(buf []byte) (interface{}, error) {
	if len(buf) < 5 {
		return nil, fmt.Errorf("invalid byte buffer it must at least have a length of 5 but: %d", len(buf))
	}
	// check magic byte
	if buf[0] != byte(0) {
		return nil, fmt.Errorf("invalid magic byte at the beginning of the buffer must be 0 but: %d", buf[0])
	}
	schemaId := binary.BigEndian.Uint32(buf[1:5])
	if c.schema.id != schemaId {
		return nil, fmt.Errorf("invalid magic byte at the beginning of the buffer must be %d but: %d", c.schema.id, schemaId)
	}
	value, _, err := c.schema.codec.NativeFromBinary(buf[5:])
	return value, err
}
