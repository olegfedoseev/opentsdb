package opentsdb

import (
	"bytes"
	"sort"
	"unicode"
	"unicode/utf8"
)

// DataPoint is a data point for the /api/put route:
// http://opentsdb.net/docs/build/html/api_http/put.html#example-single-data-point-put.
type DataPoint struct {
	Metric    string      `json:"metric"`
	Timestamp int64       `json:"timestamp"`
	Value     interface{} `json:"value"`
	Tags      Tags        `json:"tags"`
}

// DataPoints holds multiple DataPoints:
// http://opentsdb.net/docs/build/html/api_http/put.html#example-multiple-data-point-put.
type DataPoints []*DataPoint

// Tags is a helper class for tags.
type Tags map[string]string

// Set add new tag to Tags, and cleans it
func (tags Tags) Set(key, value string) {
	key = MustReplace(key, "_")
	value = MustReplace(value, "_")
	if key != "" && value != "" {
		tags[key] = value
	}
}

// String converts t to an OpenTSDB-style {a=b,c=b} string, alphabetized by key.
func (tags Tags) String() string {
	var keys []string
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	buf := bytes.NewBufferString("{")
	for i, key := range keys {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(key)
		buf.WriteString("=")
		buf.WriteString(tags[key])
	}
	buf.WriteString("}")
	return buf.String()
}

// MustReplace replace invalid, for OpenTSDB, characters from s and replace it
// with given replacement
// See: http://opentsdb.net/docs/build/html/user_guide/writing.html#metrics-and-tags
func MustReplace(s, replacement string) string {
	var c string
	for len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || r == '.' || r == '/' {
			c += string(r)
		} else {
			c += replacement
		}
		s = s[size:]
	}
	if len(c) == 0 {
		return ""
	}
	return c
}
