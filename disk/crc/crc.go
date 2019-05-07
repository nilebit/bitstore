package crc

import (
	"github.com/klauspost/crc32"
)

var table = crc32.MakeTable(crc32.Castagnoli)

type CRC uint32

func New(b []byte) CRC {
	return CRC(0).Update(b)
}

func (c CRC) Update(b []byte) CRC {
	return CRC(crc32.Update(uint32(c), table, b))
}

func (c CRC) Value() uint32 {
	return uint32(c>>15|c<<17) + 0xa282ead8
}