package opentsdb

import (
	"encoding/json"
	//	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIntegration(t *testing.T) {
	var metricsRecived int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
		body, err := gzipBodyReader(r.Body)
		assert.NoError(t, err)

		// Count how many metrics we got
		data := []interface{}{}
		err = json.Unmarshal([]byte(body), &data)
		assert.NoError(t, err)
		atomic.AddInt64(&metricsRecived, int64(len(data)))
	}))
	defer ts.Close()
	host := strings.Replace(ts.URL, "http://", "", 1)

	batchSize := 2
	httpTimeout := time.Second
	client, err := NewClient(host, batchSize, httpTimeout)
	assert.NoError(t, err)

	workerTimeout := 20 * time.Millisecond
	client.StartWorkers(4, batchSize, workerTimeout)

	// Let's push some metrics
	for i := 0; i < 99; i++ {
		err := client.Push(
			&DataPoint{"test1", 123 + int64(i), i, Tags{"key_z": "val1"}})
		time.Sleep(time.Millisecond)
		assert.NoError(t, err)
	}
	// To be sure that we get into "worker timeout" case and send 99th metric
	time.Sleep(workerTimeout)

	t.Logf("Queue: %v", len(client.Queue))
	t.Logf("Dropped: %v", client.Dropped)
	t.Logf("Sent: %v", client.Sent)
	t.Logf("Recived: %v", metricsRecived)

	select {
	case err := <-client.Errors:
		assert.NoError(t, err)
	default:
	}

	assert.EqualValues(t, 99, client.Sent)
	assert.EqualValues(t, 99, metricsRecived)
	assert.EqualValues(t, 0, client.Dropped)
}

func TestIntegrationWithFailedSend(t *testing.T) {
	var metricsRecived int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := gzipBodyReader(r.Body)
		assert.NoError(t, err)

		// Count how many metrics we got
		data := []interface{}{}
		err = json.Unmarshal([]byte(body), &data)
		assert.NoError(t, err)

		// Will fail after 10 successful POSTs
		if metricsRecived > 20 {
			w.WriteHeader(http.StatusNotFound)
		} else {
			atomic.AddInt64(&metricsRecived, int64(len(data)))
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer ts.Close()
	host := strings.Replace(ts.URL, "http://", "", 1)

	batchSize := 2
	httpTimeout := time.Second
	client, err := NewClient(host, batchSize, httpTimeout)
	assert.NoError(t, err)

	workerTimeout := 20 * time.Millisecond
	client.StartWorkers(4, batchSize, workerTimeout)

	// Let's push some metrics
	for i := 0; i < 99; i++ {
		err = client.Push(
			&DataPoint{"test1", 123 + int64(i), i, Tags{"key_z": "val1"}})
		time.Sleep(time.Millisecond)
		if err != nil {
			break
		}
	}
	assert.EqualError(t, err, "failed to push datapoint, queue is full")
	time.Sleep(workerTimeout)

	t.Logf("Queue: %v", len(client.Queue))
	t.Logf("Dropped: %v", client.Dropped)
	t.Logf("Sent: %v", client.Sent)
	t.Logf("Recived: %v", metricsRecived)

	select {
	case err = <-client.Errors:
	default:
	}
	assert.EqualError(t, err, "request failed: unexpected status 404 (\"\") (requeued 2/2)")

	assert.EqualValues(t, 22, client.Sent)
	assert.EqualValues(t, 22, metricsRecived)
	assert.True(t, client.Dropped > 0)
}

func BenchmarkWorkers1(b *testing.B) {
	runBenchmark(b, 1, 1)
}

func BenchmarkWorkers4(b *testing.B) {
	runBenchmark(b, 4, 1)
}

func BenchmarkWorkers8(b *testing.B) {
	runBenchmark(b, 8, 1)
}

func BenchmarkWorkers1_10(b *testing.B) {
	runBenchmark(b, 1, 10)
}

func BenchmarkWorkers4_10(b *testing.B) {
	runBenchmark(b, 4, 10)
}

func BenchmarkWorkers8_10(b *testing.B) {
	runBenchmark(b, 8, 10)
}

func runBenchmark(b *testing.B, workers, batchSize int) {
	var metricsRecived int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// body, err := gzipBodyReader(r.Body)
		// assert.NoError(t, err)

		// // Count how many metrics we got
		// data := []interface{}{}
		// err = json.Unmarshal([]byte(body), &data)
		// assert.NoError(t, err)
		time.Sleep(10 * time.Millisecond)

		atomic.AddInt64(&metricsRecived, 1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	host := strings.Replace(ts.URL, "http://", "", 1)

	dp := &DataPoint{"test1", 123, 1, Tags{"key_z": "val1", "key_a": "val2"}}

	httpTimeout := time.Second
	client, err := NewClient(host, batchSize, httpTimeout)
	assert.NoError(b, err)

	workerTimeout := 20 * time.Millisecond
	client.StartWorkers(workers, batchSize, workerTimeout)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Let's push some metrics
		for i := 0; i < 10000; i++ {
			for {
				if len(client.Queue) < cap(client.Queue) {
					break
				}
				time.Sleep(time.Millisecond)
			}
			err = client.Push(dp)
			assert.NoError(b, err)
		}
	}
	time.Sleep(workerTimeout)

	// b.Logf("Queue: %v", len(client.Queue))
	// b.Logf("Dropped: %v", client.Dropped)
	// b.Logf("Sent: %v", client.Sent)
	// b.Logf("Recived: %v", metricsRecived)
}
