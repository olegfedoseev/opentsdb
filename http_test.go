package opentsdb

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSendDatapoints(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)

		body, err := gzipBodyReader(r.Body)
		assert.NoError(t, err)

		expected := `[{"metric":"test1","timestamp":123,"value":1,"tags":{"key_a":"val2","key_z":"val1"}},` +
			`{"metric":"test2","timestamp":234,"value":2,"tags":{"type":"test"}}]` + "\n"
		assert.Equal(t, expected, body)
	}))
	defer ts.Close()

	dps := DataPoints{
		&DataPoint{"test1", 123, 1, Tags{"key_z": "val1", "key_a": "val2"}},
		&DataPoint{"test2", 234, 2, Tags{"type": "test"}},
	}
	assert.NoError(t, SendDataPonts(dps, ts.URL))
}

func TestSendDatapointsWithErrorAndFailedToRequeue(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "Nothing here, move along")
	}))
	defer ts.Close()
	host := strings.Replace(ts.URL, "http://", "", 1)

	client, err := NewClient(host, 1)
	assert.NoError(t, err)

	dps := DataPoints{
		&DataPoint{"test1", 123, 1, Tags{"key_z": "val1", "key_a": "val2"}},
		&DataPoint{"test2", 234, 2, Tags{"type": "test"}},
	}
	err = client.Send(dps)
	assert.Error(t, err)

	expected := "request failed: unexpected status 404 (\"Nothing here, move along\") (requeued 1/2)"
	assert.EqualError(t, err, expected)
}

func TestSendDatapointsWithError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "Nothing here, move along")
	}))
	defer ts.Close()
	host := strings.Replace(ts.URL, "http://", "", 1)

	client, err := NewClient(host, 2)
	assert.NoError(t, err)

	dps := DataPoints{
		&DataPoint{"test1", 123, 1, Tags{"key_z": "val1", "key_a": "val2"}},
		&DataPoint{"test2", 234, 2, Tags{"type": "test"}},
	}
	err = client.Send(dps)
	assert.Error(t, err)

	expected := `request failed: unexpected status 404 ("Nothing here, move along") (requeued 2/2)`
	assert.EqualError(t, err, expected)
}

func BenchmarkSend(b *testing.B) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	dps := DataPoints{
		&DataPoint{"test1", 1455942780, 1, Tags{"key_z": "val1", "key_a": "val2"}},
		&DataPoint{"test2", 1455942781, 2, Tags{"key_z1": "val1", "key_a": "val2"}},
		&DataPoint{"test3", 1455942782, 3, Tags{"key_z2": "val1ll", "key_a": "val2"}},
		&DataPoint{"test4", 1455942783, 4, Tags{"key_z3": "val1dd", "keyqq_a": "val2"}},
		&DataPoint{"test5", 1455942784, 5, Tags{"key_z4": "val1ss", "keyww_a": "val2"}},
		&DataPoint{"test6", 1455942785, 6, Tags{"key_z5": "val1dd", "keyee_a": "val2"}},
		&DataPoint{"test7", 1455942786, 7, Tags{"key_z7": "val1ff", "kerry_a": "val2"}},
		&DataPoint{"test8", 1455942787, 8, Tags{"key_z6": "val1aa", "keytt_a": "val2"}},
		&DataPoint{"test9", 1455942788, 9, Tags{"key_z8": "val1xx", "keyyy_a": "val2"}},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = SendDataPonts(dps, ts.URL)
	}
}

func gzipBodyReader(body io.ReadCloser) (string, error) {
	defer body.Close()
	g, err := gzip.NewReader(body)
	if err != nil {
		return "", err
	}

	result, err := ioutil.ReadAll(g)
	if err != nil {
		return "", err
	}

	return string(result), nil
}
