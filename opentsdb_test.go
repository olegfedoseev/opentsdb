package opentsdb

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientSend(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)

		body, err := gzipBodyReader(r.Body)
		assert.NoError(t, err)

		expected := `[{"metric":"test1","timestamp":123,"value":1,"tags":{"key_a":"val2","key_z":"val1"}},` +
			`{"metric":"test2","timestamp":234,"value":2,"tags":{"type":"test"}}]` + "\n"
		assert.Equal(t, expected, body)
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
	assert.NoError(t, err)
	assert.EqualValues(t, 2, client.Sent)
	assert.EqualValues(t, 0, client.Dropped)
}

func TestClientSendWithErrorAndFailedToRequeue(t *testing.T) {
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

func TestClientSendWithErrorAndAllRequeued(t *testing.T) {
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
