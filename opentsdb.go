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
	Queue  chan *DataPoint
	Clock  chan *Timer
	timers chan *Timer

	// Errors is channel for errors from workers, client should drain it
	Errors chan error

	// Dropped is number of dropped metrics on Push
	// If this number more than zero, than you should increase bufferSize
	Dropped int64

	// Sent is number of sent metrics by all workers from beginning of time
	Sent int64

	url string
}

// Timer is struct for passing information about "wallclock" duration of POSTing
// of batch of metrics for given timestamp
type Timer struct {
	Timestamp int64
	Start     time.Time
	Stop      time.Time
}

// NewClient will create you a new client for OpenTSDB
// host could be ip address or hostname and may contain port
// bufferSize if size of internal Queue for workers
func NewClient(host string, bufferSize int, timeout time.Duration) (*Client, error) {
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
		Clock:  make(chan *Timer, 10),
		timers: make(chan *Timer, 100),
	}
	return c, nil
}

// StartWorkers will start given number of workers that will consume and process
// metrics that you Push to client
func (client *Client) StartWorkers(workers, batchSize int, timeout time.Duration) {
	for i := 0; i < workers; i++ {
		go client.worker(batchSize, timeout)
	}
	go client.clock()
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
func (client *Client) Send(postman *Postman, batch DataPoints) error {
	if err := postman.Post(batch, client.url); err != nil {
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

func (client *Client) clock() {
	start := make(map[int64]time.Time, 0)
	stop := make(map[int64]time.Time, 0)
	var prev int64

	for {
		select {
		case timer := <-client.timers:
			if prev == 0 {
				prev = timer.Timestamp
			}

			if timer.Timestamp > prev {
				t := &Timer{
					Timestamp: prev,
					Start:     start[prev],
					Stop:      stop[prev],
				}
				client.Clock <- t

				delete(start, prev)
				delete(stop, prev)
				prev = timer.Timestamp
			}

			if _, ok := start[timer.Timestamp]; !ok {
				start[timer.Timestamp] = timer.Start
			}
			if start[timer.Timestamp].After(timer.Start) {
				start[timer.Timestamp] = timer.Start
			}
			if _, ok := stop[timer.Timestamp]; !ok {
				stop[timer.Timestamp] = timer.Stop
			}
			if stop[timer.Timestamp].Before(timer.Stop) {
				stop[timer.Timestamp] = timer.Stop
			}
		}
	}
}

func (client *Client) worker(batchSize int, timeout time.Duration) {
	buffer := make(DataPoints, 0)
	queue := make(chan DataPoints, 10)
	postman := NewPostman(timeout)

	timer := time.NewTimer(timeout)
	var prev int64
	for {
		select {
		case <-timer.C:
			if len(buffer) > 0 {
				queue <- buffer
				buffer = make(DataPoints, 0)
			}
			timer.Reset(timeout)

		case dp := <-client.Queue:
			if dp.Timestamp > prev {
				queue <- buffer
				buffer = make(DataPoints, 0)
				prev = dp.Timestamp
			}

			buffer = append(buffer, dp)
			if len(buffer) >= batchSize {
				queue <- buffer
				buffer = make(DataPoints, 0)
			}
			timer.Reset(timeout)

		case dps := <-queue:
			if len(dps) == 0 {
				continue
			}

			start := time.Now()
			if err := client.Send(postman, dps); err != nil {
				client.Errors <- err
			}

			client.timers <- &Timer{
				Timestamp: dps[0].Timestamp,
				Start:     start,
				Stop:      time.Now(),
			}
		}
	}
}
