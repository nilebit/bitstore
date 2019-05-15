package ttl

import "strconv"

type TTL struct {
	count byte
	unit  byte
}

var EMPTY_TTL = &TTL{}

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

// translate a readable ttl to internal ttl
// Supports format example:
// 3m: 3 minutes
// 4h: 4 hours
// 5d: 5 days
// 6w: 6 weeks
// 7M: 7 months
// 8y: 8 years
func ReadTTL(ttlString string) (*TTL, error) {
	if ttlString == "" {
		return EMPTY_TTL, nil
	}
	ttlBytes := []byte(ttlString)
	unitByte := ttlBytes[len(ttlBytes)-1]
	countBytes := ttlBytes[0 : len(ttlBytes)-1]
	if '0' <= unitByte && unitByte <= '9' {
		countBytes = ttlBytes
		unitByte = 'm'
	}
	count, err := strconv.Atoi(string(countBytes))
	unit := toStoredByte(unitByte)
	return &TTL{count: byte(count), unit: unit}, err
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

func toStoredByte(readableUnitByte byte) byte {
	switch readableUnitByte {
	case 'm':
		return Minute
	case 'h':
		return Hour
	case 'd':
		return Day
	case 'w':
		return Week
	case 'M':
		return Month
	case 'y':
		return Year
	}
	return 0
}
