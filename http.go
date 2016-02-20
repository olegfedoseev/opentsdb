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
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 100,
		},
	}
)

func SendDataPonts(batch DataPoints, url string) (err error) {
	resp, err := makeHTTPRequest(batch, url)
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
			return fmt.Errorf("failed to read tsdb response: %v", err)
		}
		return fmt.Errorf("unexpected status %d (%q)", resp.StatusCode, string(body))
	}
	return nil
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
