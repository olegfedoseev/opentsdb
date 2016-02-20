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

// Postman is http client for POSTing of gzip json to TSDB
type Postman struct {
	client *http.Client
	buffer bytes.Buffer
	writer *gzip.Writer
}

// NewPostman initialize http.Client and all needed buffers for new new Postman
func NewPostman(timeout time.Duration) *Postman {
	postman := &Postman{
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 100,
			},
		},
	}
	postman.writer = gzip.NewWriter(&postman.buffer)
	return postman
}

func (postman *Postman) Post(batch DataPoints, url string) (err error) {
	resp, err := postman.makeHTTPRequest(batch, url)
	if err == nil {
		// Callers should close resp.Body when done reading from it.
		defer resp.Body.Close()
	}

	if err != nil {
		return fmt.Errorf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response: %v", err)
		}
		return fmt.Errorf("unexpected status %d (%q)", resp.StatusCode, string(body))
	}
	return nil
}

func (postman *Postman) makeHTTPRequest(dps DataPoints, tsdbURL string) (*http.Response, error) {
	postman.writer.Reset(&postman.buffer)
	if err := json.NewEncoder(postman.writer).Encode(dps); err != nil {
		return nil, err
	}
	if err := postman.writer.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", tsdbURL, &postman.buffer)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	return postman.client.Do(req)
}
