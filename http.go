package opentsdb

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

var (
	httpClient = &http.Client{
		Timeout: time.Second * 5,
	}
)

func (client *Client) send(batch DataPoints) error {
	resp, err := makeHttpRequest(batch, client.url)
	if err == nil {
		client.Lock()
		client.Sent += int64(len(batch))
		client.Unlock()
		defer resp.Body.Close()
	}

	// Some problem with connecting to the server; retry later.
	if err != nil || resp.StatusCode != http.StatusNoContent {
		// requeue messages for retry
		for _, msg := range batch {
			if err := client.Push(msg); err != nil {
				break
			}
		}

		if err != nil {
			return fmt.Errorf("http request to tsdb failed: %v", err)
		} else if resp.StatusCode != http.StatusNoContent {
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("failed to read http response from tsdb: %v", err)
			}
			return fmt.Errorf("http request to tsdb failed, status %q, body %q",
				resp.Status, string(body))
		}
	}

	return nil
}

func makeHttpRequest(dps DataPoints, tsdbURL string) (*http.Response, error) {
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
