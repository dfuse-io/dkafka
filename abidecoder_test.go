package dkafka

import (
	"testing"
)

func TestABIDecoderOnReload(t *testing.T) {
	abiCodec := ABIDecoder{
		onReload: func() {},
	}
	abiCodec.onReload()
}
