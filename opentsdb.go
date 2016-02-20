package opentsdb

import (
	"fmt"
	"net"
	"net/url"
	"sync/atomic"
	"time"
)

// A Client is an OpenTSDB client. It should be created with NewClient.
type Client struct {
	url   string
	Queue chan *DataPoint

	// Errors is channel for errors from workers, client should drain it
	Errors chan error

	// Dropped is number of dropped metrics on Push
	// If this number more than zero, than you should increase bufferSize
	Dropped int64

	// Sent is number of sent metrics by all workers from beginning of time
	Sent int64
}

// NewClient will create you a new client for OpenTSDB
// host could be ip address or hostname and may contain port
// bufferSize if size of internal Queue for workers
func NewClient(host string, bufferSize int) (*Client, error) {
	if _, err := net.ResolveTCPAddr("tcp4", host); err != nil {
		return nil, fmt.Errorf("failer to resolve tsdb host %q: %v", host, err)
	}

	tsdbURL := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "api/put",
	}

	c := &Client{
		url:    tsdbURL.String(),
		Queue:  make(chan *DataPoint, bufferSize),
		Errors: make(chan error, 10),
	}
	return c, nil
}

// StartWorkers will start given number of workers that will consume and process
// metrics that you Push to client
func (client *Client) StartWorkers(workers int) {
	for i := 0; i < workers; i++ {
		go client.worker()
	}
}

// Push will add given dp to internal queue.
// If queue already full, then Push will return error
func (client *Client) Push(dp *DataPoint) error {
	select {
	case client.Queue <- dp:
	default:
		atomic.AddInt64(&client.Dropped, 1)
		return fmt.Errorf("failed to push datapoint, queue is full")
	}
	return nil
}

// Send make actual http request to send datapoint to OpenTSDB, and validates,
// that all went ok. If something is wrong it will requeue all data back to
// internal queue and return error
func (client *Client) Send(batch DataPoints) error {
	if err := SendDataPonts(batch, client.url); err != nil {
		// requeue messages for retry
		requeued := 0
		for _, msg := range batch {
			if err := client.Push(msg); err != nil {
				break
			}
			requeued++
		}
		return fmt.Errorf("request failed: %v (requeued %v/%v)", err, requeued, len(batch))
	}
	atomic.AddInt64(&client.Sent, int64(len(batch)))
	return nil
}

func (client *Client) worker() {
	buffer := make(DataPoints, 0)
	queue := make(chan DataPoints, 10)
	for {
		select {
		case <-time.After(time.Second * 10):
			if len(buffer) > 0 {
				queue <- buffer
				buffer = make(DataPoints, 0)
			}
		case dp := <-client.Queue:
			buffer = append(buffer, dp)
			if len(buffer) > 1000 {
				queue <- buffer
				buffer = make(DataPoints, 0)
			}
		case dps := <-queue:
			t := time.Now()
			if err := client.Send(dps); err != nil {
				client.Errors <- err
			}
		}
	}
}
