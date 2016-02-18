package opentsdb

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync/atomic"
	"time"
)

var (
	httpClient = &http.Client{
		Timeout: time.Second * 5,
	}
)

// Send make actual http request to send datapoint to OpenTSDB, and validates,
// that all went ok. If something is wrong it will requeue all data back to
// internal queue and return error
func (client *Client) Send(batch DataPoints) {
	resp, err := makeHTTPRequest(batch, client.url)
	if err == nil {
		atomic.AddInt64(&client.Sent, int64(len(batch)))

		defer func() {
			// Callers should close resp.Body when done reading from it.
			if err := resp.Body.Close(); err != nil {
				client.Errors <- fmt.Errorf("failed to close resp.Body: %v", err)
			}
		}()
	}

	// requeue messages for retry
	for idx, msg := range batch {
		if err := client.Push(msg); err != nil {
			client.Errors <- fmt.Errorf("failed to requeue all datapoint %v/%v: %v",
				idx, len(batch), err)
			break
		}
	}

	if err != nil {
		client.Errors <- fmt.Errorf("http request to tsdb failed: %v", err)
		return
	}

	if resp.StatusCode != http.StatusNoContent {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			client.Errors <- fmt.Errorf("failed to read http response from tsdb: %v", err)
			return
		}
		client.Errors <- fmt.Errorf(
			"http request to tsdb failed, status %q, body %q",
			resp.Status, string(body))
	}
}

func makeHTTPRequest(dps DataPoints, tsdbURL string) (*http.Response, error) {
	var buf bytes.Buffer
	g := gzip.NewWriter(&buf)
	if err := json.NewEncoder(g).Encode(dps); err != nil {
		return nil, err
	}
	if err := g.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", tsdbURL, &buf)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	return httpClient.Do(req)
}
