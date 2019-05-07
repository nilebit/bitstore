package ttl

import "strconv"

type TTL struct {
	count byte
	unit  byte
}

const (
	//stored unit types
	Empty byte = iota
	Minute
	Hour
	Day
	Week
	Month
	Year
)

// read stored bytes to a ttl
func LoadFromBytes(input []byte) (t *TTL) {
	return &TTL{count: input[0], unit: input[1]}
}

// save stored bytes to an output with 2 bytes
func (t *TTL) ToBytes(output []byte) {
	output[0] = t.count
	output[1] = t.unit
}


func (t *TTL) String() string {
	if t == nil || t.count == 0 {
		return ""
	}
	if t.unit == Empty {
		return ""
	}
	countString := strconv.Itoa(int(t.count))
	switch t.unit {
	case Minute:
		return countString + "m"
	case Hour:
		return countString + "h"
	case Day:
		return countString + "d"
	case Week:
		return countString + "w"
	case Month:
		return countString + "M"
	case Year:
		return countString + "y"
	}
	return ""
}