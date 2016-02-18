package opentsdb

import (
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"
)

type Client struct {
	sync.Mutex

	url     string
	Queue   chan *DataPoint
	Dropped int64
	Sent    int64
}

func NewClient(host string, bufferSize, workers int) (*Client, error) {
	if _, err := net.ResolveTCPAddr("tcp4", host); err != nil {
		return nil, fmt.Errorf("failer to resolve tsdb host %q: %v", host, err)
	}

	tsdbURL := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "api/put",
	}

	c := &Client{
		url:   tsdbURL.String(),
		Queue: make(chan *DataPoint, bufferSize),
	}
	for i := 0; i < workers; i++ {
		go c.Worker()
	}
	return c, nil
}

func (client *Client) Push(dp *DataPoint) error {
	select {
	case client.Queue <- dp:
	default:
		client.Lock()
		client.Dropped++
		client.Unlock()
		return fmt.Errorf("failed to push datapoint, queue is full!")
	}
	return nil
}

func (client *Client) Worker() {
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
			go client.send(dps)
		}
	}
}
