package reader

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetOffsetFromQualifier(t *testing.T) {
	assert.EqualValues(t, 3, getOffsetFromQualifier([]byte{0x00, 0x37}, 0))
}

// func TestGetOffsetFromQualifierMs1ms(t *testing.T) {
// 	assert.EqualValues(t, 1, getOffsetFromQualifier([]byte{0xF0, 0x00, 0x00, 0x47}, 0))
// }

// func TestGetOffsetFromQualifierMs(t *testing.T) {
// 	assert.EqualValues(t, 8, getOffsetFromQualifier([]byte{0xF0, 0x00, 0x02, 0x07}, 0))
// }

// func TestGetOffsetFromQualifierWithOffset(t *testing.T) {
// 	assert.EqualValues(t, 12, getOffsetFromQualifier([]byte{0xF0, 0x00, 0x03, 0x07}, 0))
// }

func TestExtractQualifierSeconds(t *testing.T) {
	qual := []byte{0x00, 0x37, 0xF0, 0x00, 0x02, 0x07, 0x00, 0x47}
	assert.EqualValues(t, []byte{0, 0x47}, extractQualifier(qual, 6))
}

func TestExtractQualifierMilliSeconds(t *testing.T) {
	qual := []byte{0x00, 0x37, 0xF0, 0x00, 0x02, 0x07, 0x00, 0x47}
	assert.EqualValues(t, []byte{0xF0, 0x00, 0x02, 0x07}, extractQualifier(qual, 2))
}

func TestFixFloatingPointValue(t *testing.T) {
	assert.EqualValues(t, []byte{0, 0, 0, 1}, fixFloatingPointValue(0x0B, []byte{0, 0, 0, 0, 0, 0, 0, 1}))
}

func TestFixFloatingPointValueNot(t *testing.T) {
	assert.EqualValues(t, []byte{0, 0, 0, 1}, fixFloatingPointValue(0x0B, []byte{0, 0, 0, 1}))
}

func TestFixFloatingPointValueWasInt(t *testing.T) {
	assert.EqualValues(t, []byte{0, 0, 0, 1}, fixFloatingPointValue(0x03, []byte{0, 0, 0, 1}))
}

// (expected = IllegalDataException.class)
func TestFixFloatingPointValueCorrupt(t *testing.T) {
	fixFloatingPointValue(0x0B, []byte{0, 2, 0, 0, 0, 0, 0, 1})
}

func TestGetValueLengthFromQualifierInt8(t *testing.T) {
	assert.EqualValues(t, 8, getValueLengthFromQualifier([]byte{0, 7}, 0))
}

func TestGetValueLengthFromQualifierInt8also(t *testing.T) {
	assert.EqualValues(t, 8, getValueLengthFromQualifier([]byte{0, 0x0F}, 0))
}

func TestGetValueLengthFromQualifierInt1(t *testing.T) {
	assert.EqualValues(t, 1, getValueLengthFromQualifier([]byte{0, 0}, 0))
}

func TestGetValueLengthFromQualifierInt4(t *testing.T) {
	assert.EqualValues(t, 4, getValueLengthFromQualifier([]byte{0, 0x4B}, 0))
}

func TestGetValueLengthFromQualifierFloat4(t *testing.T) {
	assert.EqualValues(t, 4, getValueLengthFromQualifier([]byte{0, 11}, 0))
}

func TestGetValueLengthFromQualifierFloat4also(t *testing.T) {
	assert.EqualValues(t, 4, getValueLengthFromQualifier([]byte{0, 0x1B}, 0))
}

func TestGetValueLengthFromQualifierFloat8(t *testing.T) {
	assert.EqualValues(t, 8, getValueLengthFromQualifier([]byte{0, 0x1F}, 0))
}

func TestGetFlagsFromQualifierInt(t *testing.T) {
	t.Logf("int: %#v", getFlagsFromQualifier([]byte{0x00, 0x37}, 0)&FLAG_FLOAT)
	assert.EqualValues(t, 7, getFlagsFromQualifier([]byte{0x00, 0x37}, 0))
}

func TestGetFlagsFromQualifierFloat(t *testing.T) {
	t.Logf("float: %#v", getFlagsFromQualifier([]byte{0x00, 0x37}, 0)&FLAG_FLOAT)
	assert.EqualValues(t, 11, getFlagsFromQualifier([]byte{0x00, 0x1B}, 0))
}
