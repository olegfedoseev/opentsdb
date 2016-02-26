package opentsdb

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	dps = DataPoints{
		&DataPoint{"test1", 123, 1, Tags{"key_z": "val1", "key_a": "val2"}},
		&DataPoint{"test2", 234, 2, Tags{"type": "test"}},
	}
)

func TestClientSend(t *testing.T) {
	expected := `[{"metric":"test1","timestamp":123,"value":1,"tags":{"key_a":"val2","key_z":"val1"}},` +
		`{"metric":"test2","timestamp":234,"value":2,"tags":{"type":"test"}}]` + "\n"
	ts, host := createTestServer(expected, t)
	defer ts.Close()

	client, err := NewClient(host, 1, 5*time.Second)
	assert.NoError(t, err)

	postman := NewPostman(time.Second)
	err = client.Send(postman, dps)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, client.Sent)
	assert.EqualValues(t, 0, client.Dropped)
}

func TestNewClientWithInvalidHost(t *testing.T) {
	client, err := NewClient("***111", 1, time.Second)
	assert.Nil(t, client)
	expected := "failer to resolve tsdb host \"***111\": missing port in address ***111"
	assert.EqualError(t, err, expected)
}

func TestClientSendWithErrorAndFailedToRequeue(t *testing.T) {
	ts, host := createTestServerWith404()
	defer ts.Close()

	// bufferSize is less than len(dps) == 2
	bufferSize := 1
	client, err := NewClient(host, bufferSize, 5*time.Second)
	assert.NoError(t, err)

	postman := NewPostman(time.Second)
	err = client.Send(postman, dps)
	assert.Error(t, err)

	expected := `request failed: unexpected status 404 ("Nothing here, move along") (requeued 1/2)`
	assert.EqualError(t, err, expected)
}

func TestClientSendWithErrorAndAllRequeued(t *testing.T) {
	ts, host := createTestServerWith404()
	defer ts.Close()

	// bufferSize is equal to len(dps) == 2
	bufferSize := 2
	client, err := NewClient(host, bufferSize, 5*time.Second)
	assert.NoError(t, err)

	postman := NewPostman(time.Second)
	err = client.Send(postman, dps)
	assert.Error(t, err)

	expected := `request failed: unexpected status 404 ("Nothing here, move along") (requeued 2/2)`
	assert.EqualError(t, err, expected)
}

func createTestServer(expected string, t *testing.T) (*httptest.Server, string) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
		body, err := gzipBodyReader(r.Body)

		assert.NoError(t, err)
		assert.Equal(t, expected, body)
	}))
	return ts, strings.Replace(ts.URL, "http://", "", 1)
}

func createTestServerWith404() (*httptest.Server, string) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "Nothing here, move along")
	}))
	return ts, strings.Replace(ts.URL, "http://", "", 1)
}
