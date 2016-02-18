package opentsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTagsSetWithCleanup(t *testing.T) {
	tags := make(Tags)
	tags.Set("key][2", "baz@@eee")
	tags.Set("key||3", "fizz%ddd")
	tags.Set("123::key", "foo,,bar")

	assert.Equal(t, "{123__key=foo__bar,key__2=baz__eee,key__3=fizz_ddd}", tags.String())
}

func TestTagsSetWithEmptyValue(t *testing.T) {
	tags := make(Tags)
	tags.Set("key", "%%%%")
	tags.Set("][", "val")
	tags.Set("key2", "")
	tags.Set("", "val2")

	assert.Equal(t, "{__=val,key=____}", tags.String())
}

func TestTagsToString(t *testing.T) {
	tags := make(Tags)
	tags.Set("key5", "val5")
	tags.Set("key2", "val2")
	tags.Set("key4", "val4")
	tags.Set("key1", "val1")
	tags.Set("key3", "val3")

	assert.Equal(t, "{key1=val1,key2=val2,key3=val3,key4=val4,key5=val5}", tags.String())
}

func BenchmarkTagsToString(b *testing.B) {
	tags := make(Tags)
	tags.Set("key5", "val5")
	tags.Set("key2", "val2")
	tags.Set("key4", "val4")
	tags.Set("key1", "val1")
	tags.Set("key3", "val3")
	tags.Set("key6", "val4")
	tags.Set("key7", "val1")
	tags.Set("key8", "val3")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tags.String()
	}
}
