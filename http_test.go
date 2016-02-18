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
	host := strings.Replace(ts.URL, "http://", "", 1)

	client, err := NewClient(host, 1)
	assert.NoError(t, err)

	dps := DataPoints{
		&DataPoint{"test1", 123, 1, Tags{"key_z": "val1", "key_a": "val2"}},
		&DataPoint{"test2", 234, 2, Tags{"type": "test"}},
	}
	client.Send(dps)
}

func TestSendDatapointsWithErrorAndFailedToRequeue(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(w, "Nothing here, move along")
	}))
	defer ts.Close()
	host := strings.Replace(ts.URL, "http://", "", 1)

	client, err := NewClient(host, 1)
	assert.NoError(t, err)

	dps := DataPoints{
		&DataPoint{"test1", 123, 1, Tags{"key_z": "val1", "key_a": "val2"}},
		&DataPoint{"test2", 234, 2, Tags{"type": "test"}},
	}
	client.Send(dps)

	select {
	case err = <-client.Errors:
	default:
	}

	assert.Error(t, err)
	expected := "failed to requeue all datapoint 1/2: failed to push datapoint, queue is full"
	assert.EqualError(t, err, expected)
}

func TestSendDatapointsWithError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(w, "Nothing here, move along")
	}))
	defer ts.Close()
	host := strings.Replace(ts.URL, "http://", "", 1)

	client, err := NewClient(host, 2)
	assert.NoError(t, err)

	dps := DataPoints{
		&DataPoint{"test1", 123, 1, Tags{"key_z": "val1", "key_a": "val2"}},
		&DataPoint{"test2", 234, 2, Tags{"type": "test"}},
	}
	client.Send(dps)

	select {
	case err = <-client.Errors:
	default:
	}

	assert.Error(t, err)
	expected := `http request to tsdb failed, status "404 Not Found", body "Nothing here, move along\n"`
	assert.EqualError(t, err, expected)
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
