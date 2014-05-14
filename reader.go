package nsq

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Handler is the synchronous interface to Reader.
//
// Implement this interface for handlers that return whether or not message
// processing completed successfully.
//
// When the return value is nil Reader will automatically handle FINishing.
//
// When the returned value is non-nil Reader will automatically handle REQueing.
type Handler interface {
	HandleMessage(message *Message) error
}

// AsyncHandler is the asynchronous interface to Reader.
//
// Implement this interface for handlers that wish to defer responding until later.
// This is particularly useful if you want to batch work together.
//
// An AsyncHandler must send:
//
//     &FinishedMessage{messageID, requeueDelay, true|false}
//
// To the supplied responseChan to indicate that a message is processed.
type AsyncHandler interface {
	HandleMessage(message *Message, responseChan chan *FinishedMessage)
}

// FinishedMessage is the data type used over responseChan in AsyncHandlers
type FinishedMessage struct {
	Id             MessageID
	RequeueDelayMs int
	Success        bool
}

// FailedMessageLogger is an interface that can be implemented by handlers that wish
// to receive a callback when a message is deemed "failed" (i.e. the number of attempts
// exceeded the Reader specified MaxAttemptCount)
type FailedMessageLogger interface {
	LogFailedMessage(message *Message)
}

// Reader is a high-level type to consume from NSQ.
//
// A Reader instance is supplied handler(s) that will be executed
// concurrently via goroutines to handle processing the stream of messages
// consumed from the specified topic/channel.  See: AsyncHandler and Handler
// for details on implementing those interfaces to create handlers.
//
// If configured, it will poll nsqlookupd instances and handle connection (and
// reconnection) to any discovered nsqds.
type Reader struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	MessagesReceived uint64 // an atomic counter - # of messages received
	MessagesFinished uint64 // an atomic counter - # of messages FINished
	MessagesRequeued uint64 // an atomic counter - # of messages REQueued
	totalRdyCount    int64
	backoffDuration  int64

	sync.RWMutex

	// basics
	TopicName      string   // name of topic to subscribe to
	ChannelName    string   // name of channel to subscribe to
	VerboseLogging bool     // enable verbose logging
	ExitChan       chan int // read from this channel to block your main loop

	// network deadlines
	ReadTimeout  time.Duration // the deadline set for network reads
	WriteTimeout time.Duration // the deadline set for network writes

	// lookupd
	LookupdPollInterval time.Duration // duration between polling lookupd for new connections
	LookupdPollJitter   float64       // Maximum fractional amount of jitter to add to the lookupd pool loop. This helps evenly distribute requests even if multiple consumers restart at the same time.

	// requeue delays
	MaxRequeueDelay     time.Duration // the maximum duration when REQueueing (for doubling of deferred requeue)
	DefaultRequeueDelay time.Duration // the default duration when REQueueing
	BackoffMultiplier   time.Duration // the unit of time for calculating reader backoff

	// misc
	MaxAttemptCount   uint16        // maximum number of times this reader will attempt to process a message
	LowRdyIdleTimeout time.Duration // the amount of time in seconds to wait for a message from a producer when in a state where RDY counts are re-distributed (ie. max_in_flight < num_producers)

	ShortIdentifier string // an identifier to send to nsqd when connecting (defaults: short hostname)
	LongIdentifier  string // an identifier to send to nsqd when connecting (defaults: long hostname)

	HeartbeatInterval time.Duration // duration of time between heartbeats
	SampleRate        int32         // set the sampleRate of the client's messagePump (requires nsqd 0.2.25+)
	UserAgent         string        // a string identifying the agent for this client in the spirit of HTTP (default: "<client_library_name>/<version>")

	// transport layer security
	TLSv1     bool        // negotiate enabling TLS
	TLSConfig *tls.Config // client TLS configuration

	// compression
	Deflate      bool // negotiate enabling Deflate compression
	DeflateLevel int  // the compression level to negotiate for Deflate
	Snappy       bool // negotiate enabling Snappy compression

	// output buffering
	OutputBufferSize    int64         // size of the buffer (in bytes) used by nsqd for buffering writes to this connection
	OutputBufferTimeout time.Duration // timeout (in ms) used by nsqd before flushing buffered writes (set to 0 to disable). Warning: configuring clients with an extremely low (< 25ms) output_buffer_timeout has a significant effect on nsqd CPU usage (particularly with > 50 clients connected).

	// internal variables
	maxBackoffDuration   time.Duration
	maxBackoffCount      int32
	backoffChan          chan bool
	rdyChan              chan *Conn
	needRDYRedistributed int32
	backoffCounter       int32

	maxInFlightMutex sync.RWMutex
	maxInFlight      int

	incomingMessages chan *Message

	rdyRetryTimers     map[string]*time.Timer
	pendingConnections map[string]bool
	connections        map[string]*Conn

	lookupdRecheckChan chan int
	lookupdHTTPAddrs   []string
	lookupdQueryIndex  int

	runningHandlers int32
	stopFlag        int32
	stopHandler     sync.Once
}

// NewReader creates a new instance of Reader for the specified topic/channel
//
// The returned Reader instance is setup with sane default values.  To modify
// configuration, update the values on the returned instance before connecting.
func NewReader(topic string, channel string) (*Reader, error) {
	if !IsValidTopicName(topic) {
		return nil, errors.New("invalid topic name")
	}

	if !IsValidChannelName(channel) {
		return nil, errors.New("invalid channel name")
	}

	q := &Reader{
		TopicName:   topic,
		ChannelName: channel,

		MaxAttemptCount: 5,

		LookupdPollInterval: 60 * time.Second,
		LookupdPollJitter:   0.3,

		LowRdyIdleTimeout: 10 * time.Second,

		DefaultRequeueDelay: 90 * time.Second,
		MaxRequeueDelay:     15 * time.Minute,
		BackoffMultiplier:   time.Second,

		ReadTimeout:  DefaultClientTimeout,
		WriteTimeout: time.Second,

		incomingMessages: make(chan *Message),

		rdyRetryTimers:     make(map[string]*time.Timer),
		pendingConnections: make(map[string]bool),
		connections:        make(map[string]*Conn),

		lookupdRecheckChan: make(chan int, 1), // used at connection close to force a possible reconnect
		maxInFlight:        1,
		backoffChan:        make(chan bool),
		rdyChan:            make(chan *Conn, 1),

		ExitChan: make(chan int),
	}
	q.SetMaxBackoffDuration(120 * time.Second)
	go q.rdyLoop()
	return q, nil
}

func (q *Reader) conns() []*Conn {
	q.RLock()
	conns := make([]*Conn, 0, len(q.connections))
	for _, c := range q.connections {
		conns = append(conns, c)
	}
	q.RUnlock()
	return conns
}

// ConnectionMaxInFlight calculates the per-connection max-in-flight count.
//
// This may change dynamically based on the number of connections to nsqd the Reader
// is responsible for.
func (q *Reader) ConnectionMaxInFlight() int64 {
	b := float64(q.MaxInFlight())
	s := b / float64(len(q.conns()))
	return int64(math.Min(math.Max(1, s), b))
}

// IsStarved indicates whether any connections for this reader are blocked on processing
// before being able to receive more messages (ie. RDY count of 0 and not exiting)
func (q *Reader) IsStarved() bool {
	for _, conn := range q.conns() {
		threshold := int64(float64(atomic.LoadInt64(&conn.lastRdyCount)) * 0.85)
		inFlight := atomic.LoadInt64(&conn.messagesInFlight)
		if inFlight >= threshold && inFlight > 0 && atomic.LoadInt32(&conn.stopFlag) != 1 {
			return true
		}
	}
	return false
}

// SetMaxInFlight sets the maximum number of messages this reader instance
// will allow in-flight.
//
// If already connected, it updates the reader RDY state for each connection.
func (q *Reader) SetMaxInFlight(maxInFlight int) {
	if atomic.LoadInt32(&q.stopFlag) == 1 {
		return
	}

	q.maxInFlightMutex.Lock()
	if maxInFlight == q.maxInFlight {
		q.maxInFlightMutex.Unlock()
		return
	}
	q.maxInFlight = maxInFlight
	q.maxInFlightMutex.Unlock()

	for _, c := range q.conns() {
		q.rdyChan <- c
	}
}

// SetMaxBackoffDuration sets the maximum duration a connection will backoff from message processing
func (q *Reader) SetMaxBackoffDuration(duration time.Duration) {
	q.maxBackoffDuration = duration
	atomic.StoreInt32(&q.maxBackoffCount,
		int32(math.Max(1, math.Ceil(math.Log2(duration.Seconds())))))
}

// MaxInFlight returns the configured maximum number of messages to allow in-flight.
func (q *Reader) MaxInFlight() int {
	q.maxInFlightMutex.RLock()
	mif := q.maxInFlight
	q.maxInFlightMutex.RUnlock()
	return mif
}

// ConnectToLookupd adds an nsqlookupd address to the list for this Reader instance.
//
// If it is the first to be added, it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (q *Reader) ConnectToLookupd(addr string) error {
	q.Lock()
	for _, x := range q.lookupdHTTPAddrs {
		if x == addr {
			q.Unlock()
			return ErrLookupdAddressExists
		}
	}
	q.lookupdHTTPAddrs = append(q.lookupdHTTPAddrs, addr)
	numLookupd := len(q.lookupdHTTPAddrs)
	q.Unlock()

	// if this is the first one, kick off the go loop
	if numLookupd == 1 {
		q.queryLookupd()
		go q.lookupdLoop()
	}

	return nil
}

// poll all known lookup servers every LookupdPollInterval
func (q *Reader) lookupdLoop() {
	// add some jitter so that multiple consumers discovering the same topic,
	// when restarted at the same time, dont all connect at once.
	rand.Seed(time.Now().UnixNano())

	jitter := time.Duration(int64(rand.Float64() * q.LookupdPollJitter * float64(q.LookupdPollInterval)))
	ticker := time.NewTicker(q.LookupdPollInterval)

	select {
	case <-time.After(jitter):
	case <-q.ExitChan:
		goto exit
	}

	for {
		select {
		case <-ticker.C:
			q.queryLookupd()
		case <-q.lookupdRecheckChan:
			q.queryLookupd()
		case <-q.ExitChan:
			goto exit
		}
	}

exit:
	ticker.Stop()
	log.Printf("exiting lookupdLoop")
}

// make an HTTP req to the /lookup endpoint of one of the
// configured nsqlookupd instances to discover which nsqd provide
// the topic we are consuming.
//
// initiate a connection to any new producers that are identified.
func (q *Reader) queryLookupd() {
	q.RLock()
	addr := q.lookupdHTTPAddrs[q.lookupdQueryIndex]
	num := len(q.lookupdHTTPAddrs)
	q.RUnlock()
	q.lookupdQueryIndex = (q.lookupdQueryIndex + 1) % num
	endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(q.TopicName))

	log.Printf("LOOKUPD: querying %s", endpoint)

	data, err := ApiRequest(endpoint)
	if err != nil {
		log.Printf("ERROR: lookupd %s - %s", addr, err.Error())
		return
	}

	// {
	//     "data": {
	//         "channels": [],
	//         "producers": [
	//             {
	//                 "broadcast_address": "jehiah-air.local",
	//                 "http_port": 4151,
	//                 "tcp_port": 4150
	//             }
	//         ],
	//         "timestamp": 1340152173
	//     },
	//     "status_code": 200,
	//     "status_txt": "OK"
	// }
	for i, _ := range data.Get("producers").MustArray() {
		producer := data.Get("producers").GetIndex(i)
		address := producer.Get("address").MustString()
		broadcastAddress, ok := producer.CheckGet("broadcast_address")
		if ok {
			address = broadcastAddress.MustString()
		}
		port := producer.Get("tcp_port").MustInt()

		// make an address, start a connection
		joined := net.JoinHostPort(address, strconv.Itoa(port))
		err = q.ConnectToNSQ(joined)
		if err != nil && err != ErrAlreadyConnected {
			log.Printf("ERROR: failed to connect to nsqd (%s) - %s", joined, err.Error())
			continue
		}
	}
}

// ConnectToNSQ takes a nsqd address to connect directly to.
//
// It is recommended to use ConnectToLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to a single, local,
// instance.
func (q *Reader) ConnectToNSQ(addr string) error {
	if atomic.LoadInt32(&q.stopFlag) == 1 {
		return errors.New("reader stopped")
	}

	if atomic.LoadInt32(&q.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	_, pendingOk := q.pendingConnections[addr]
	q.RLock()
	_, ok := q.connections[addr]
	q.RUnlock()

	if ok || pendingOk {
		return ErrAlreadyConnected
	}

	log.Printf("[%s] connecting to nsqd", addr)

	conn := NewConn(addr, q.TopicName, q.ChannelName)
	if q.ReadTimeout > 0 {
		conn.ReadTimeout = q.ReadTimeout
	}
	if q.WriteTimeout > 0 {
		conn.WriteTimeout = q.WriteTimeout
	}
	conn.Deflate = q.Deflate
	if q.DeflateLevel > 0 {
		conn.DeflateLevel = q.DeflateLevel
	}
	conn.Snappy = q.Snappy
	conn.TLSv1 = q.TLSv1
	conn.TLSConfig = q.TLSConfig
	if q.ShortIdentifier != "" {
		conn.ShortIdentifier = q.ShortIdentifier
	}
	if q.LongIdentifier != "" {
		conn.LongIdentifier = q.LongIdentifier
	}
	if q.HeartbeatInterval != 0 {
		conn.HeartbeatInterval = q.HeartbeatInterval
	}
	if q.SampleRate > 0 {
		conn.SampleRate = q.SampleRate
	}
	if q.UserAgent != "" {
		conn.UserAgent = q.UserAgent
	}
	if q.OutputBufferSize != 0 {
		conn.OutputBufferSize = q.OutputBufferSize
	}
	if q.OutputBufferTimeout != 0 {
		conn.OutputBufferTimeout = q.OutputBufferTimeout
	}
	conn.MessageCB = func(c *Conn, msg *Message) {
		q.onConnectionMessage(c, msg)
	}
	conn.MessageProcessedCB = func(c *Conn, finishedMsg *FinishedMessage) {
		q.onConnectionMessageProcessed(c, finishedMsg)
	}
	conn.ResponseCB = func(c *Conn, data []byte) {
		q.onConnectionResponse(c, data)
	}
	conn.ErrorCB = func(c *Conn, data []byte) {
		q.onConnectionError(c, data)
	}
	conn.HeartbeatCB = func(c *Conn) {
		q.onConnectionHeartbeat(c)
	}
	conn.IOErrorCB = func(c *Conn, err error) {
		q.onConnectionIOError(c, err)
	}
	conn.CloseCB = func(c *Conn) {
		q.onConnectionClosed(c)
	}

	cleanupConnection := func() {
		q.Lock()
		delete(q.pendingConnections, addr)
		q.Unlock()
		conn.Close()
	}

	q.pendingConnections[addr] = true

	resp, err := conn.Connect()
	if err != nil {
		cleanupConnection()
		return err
	}

	if resp != nil {
		log.Printf("[%s] IDENTIFY response: %+v", conn, resp)
		if resp.MaxRdyCount < int64(q.MaxInFlight()) {
			log.Printf("[%s] max RDY count %d < reader max in flight %d, truncation possible",
				conn, resp.MaxRdyCount, q.MaxInFlight())
		}
		if resp.TLSv1 {
			log.Printf("[%s] upgrading to TLS", conn)
		}
		if resp.Deflate {
			log.Printf("[%s] upgrading to Deflate", conn)
		}
		if resp.Snappy {
			log.Printf("[%s] upgrading to Snappy", conn)
		}
	}

	cmd := Subscribe(q.TopicName, q.ChannelName)
	err = conn.SendCommand(cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to subscribe to %s:%s - %s",
			conn, q.TopicName, q.ChannelName, err.Error())
	}

	delete(q.pendingConnections, addr)
	q.Lock()
	q.connections[addr] = conn
	q.Unlock()

	// pre-emptive signal to existing connections to lower their RDY count
	for _, c := range q.conns() {
		q.rdyChan <- c
	}

	return nil
}

func (q *Reader) onConnectionMessage(c *Conn, msg *Message) {
	atomic.AddInt64(&q.totalRdyCount, -1)
	atomic.AddUint64(&q.MessagesReceived, 1)
	q.incomingMessages <- msg
	q.rdyChan <- c
}

func (q *Reader) onConnectionMessageProcessed(c *Conn, finishedMsg *FinishedMessage) {
	if finishedMsg.Success {
		if q.VerboseLogging {
			log.Printf("[%s] finishing %s", c, finishedMsg.Id)
		}
		atomic.AddUint64(&q.MessagesFinished, 1)
	} else {
		if q.VerboseLogging {
			log.Printf("[%s] requeuing %s", c, finishedMsg.Id)
		}
		atomic.AddUint64(&q.MessagesRequeued, 1)
	}
	q.backoffChan <- finishedMsg.Success
}

func (q *Reader) onConnectionResponse(c *Conn, data []byte) {
	switch {
	case bytes.Equal(data, []byte("CLOSE_WAIT")):
		// server is ready for us to close (it ack'd our StartClose)
		// we can assume we will not receive any more messages over this channel
		// (but we can still write back responses)
		log.Printf("[%s] received ACK from nsqd - now in CLOSE_WAIT", c)
		c.Stop()
	}
}

func (q *Reader) onConnectionError(c *Conn, data []byte) {
	log.Printf("[%s] error from nsqd %s", c, data)
}

func (q *Reader) onConnectionHeartbeat(c *Conn) {
	log.Printf("[%s] heartbeat received", c)
}

func (q *Reader) onConnectionIOError(c *Conn, err error) {
	log.Printf("[%s] IO Error - %s", c, err)
	c.Stop()
}

func (q *Reader) onConnectionClosed(c *Conn) {
	var hasRDYRetryTimer bool

	// remove this connections RDY count from the reader's total
	rdyCount := c.RDY()
	atomic.AddInt64(&q.totalRdyCount, -rdyCount)

	c.Lock()
	if timer, ok := q.rdyRetryTimers[c.String()]; ok {
		// stop any pending retry of an old RDY update
		timer.Stop()
		delete(q.rdyRetryTimers, c.String())
		hasRDYRetryTimer = true
	}
	c.Unlock()

	q.Lock()
	delete(q.connections, c.Address())
	left := len(q.connections)
	q.Unlock()

	log.Printf("there are %d connections left alive", left)

	if (hasRDYRetryTimer || rdyCount > 0) &&
		(left == q.MaxInFlight() || q.inBackoff()) {
		// we're toggling out of (normal) redistribution cases and this conn
		// had a RDY count...
		//
		// trigger RDY redistribution to make sure this RDY is moved
		// to a new connection
		atomic.StoreInt32(&q.needRDYRedistributed, 1)
	}

	// we were the last one (and stopping)
	if left == 0 && atomic.LoadInt32(&q.stopFlag) == 1 {
		q.stopHandlers()
		return
	}

	q.RLock()
	numLookupd := len(q.lookupdHTTPAddrs)
	q.RUnlock()
	if numLookupd != 0 && atomic.LoadInt32(&q.stopFlag) == 0 {
		// trigger a poll of the lookupd
		select {
		case q.lookupdRecheckChan <- 1:
		default:
		}
	} else if numLookupd == 0 && atomic.LoadInt32(&q.stopFlag) == 0 {
		// there are no lookupd, try to reconnect after a bit
		go func(addr string) {
			for {
				log.Printf("[%s] re-connecting in 15 seconds...", addr)
				time.Sleep(15 * time.Second)
				if atomic.LoadInt32(&q.stopFlag) == 1 {
					break
				}
				err := q.ConnectToNSQ(addr)
				if err != nil && err != ErrAlreadyConnected {
					log.Printf("ERROR: failed to connect to %s - %s",
						addr, err.Error())
					continue
				}
				break
			}
		}(c.RemoteAddr().String())
	}
}

func (q *Reader) backoffDurationForCount(count int32) time.Duration {
	backoffDuration := q.BackoffMultiplier * time.Duration(math.Pow(2, float64(count)))
	if backoffDuration > q.maxBackoffDuration {
		backoffDuration = q.maxBackoffDuration
	}
	return backoffDuration
}

func (q *Reader) inBackoff() bool {
	return atomic.LoadInt32(&q.backoffCounter) > 0
}

func (q *Reader) inBackoffBlock() bool {
	return atomic.LoadInt64(&q.backoffDuration) > 0
}

func (q *Reader) rdyLoop() {
	var backoffTimer *time.Timer
	var backoffTimerChan <-chan time.Time
	var backoffCounter int32

	redistributeTicker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-backoffTimerChan:
			var choice *Conn

			backoffTimer = nil
			backoffTimerChan = nil
			atomic.StoreInt64(&q.backoffDuration, 0)

			// pick a random connection to test the waters
			var i int
			conns := q.conns()
			if len(conns) == 0 {
				continue
			}
			idx := rand.Intn(len(conns))
			for _, c := range conns {
				if i == idx {
					choice = c
					break
				}
				i++
			}

			log.Printf("[%s] backoff time expired, continuing with RDY 1...", choice)
			// while in backoff only ever let 1 message at a time through
			q.updateRDY(choice, 1)
		case c := <-q.rdyChan:
			if backoffTimer != nil || backoffCounter > 0 {
				continue
			}

			// send ready immediately
			remain := c.RDY()
			lastRdyCount := c.LastRDY()
			count := q.ConnectionMaxInFlight()
			// refill when at 1, or at 25%, or if connections have changed and we have too many RDY
			if remain <= 1 || remain < (lastRdyCount/4) || (count > 0 && count < remain) {
				if q.VerboseLogging {
					log.Printf("[%s] sending RDY %d (%d remain from last RDY %d)",
						c, count, remain, lastRdyCount)
				}
				q.updateRDY(c, count)
			} else {
				if q.VerboseLogging {
					log.Printf("[%s] skip sending RDY %d (%d remain out of last RDY %d)",
						c, count, remain, lastRdyCount)
				}
			}
		case success := <-q.backoffChan:
			// prevent many async failures/successes from immediately resulting in
			// max backoff/normal rate (by ensuring that we dont continually incr/decr
			// the counter during a backoff period)
			if backoffTimer != nil {
				continue
			}

			// update backoff state
			backoffUpdated := false
			if success {
				if backoffCounter > 0 {
					backoffCounter--
					backoffUpdated = true
				}
			} else {
				if backoffCounter < atomic.LoadInt32(&q.maxBackoffCount) {
					backoffCounter++
					backoffUpdated = true
				}
			}

			if backoffUpdated {
				atomic.StoreInt32(&q.backoffCounter, backoffCounter)
			}

			// exit backoff
			if backoffCounter == 0 && backoffUpdated {
				count := q.ConnectionMaxInFlight()
				for _, c := range q.conns() {
					if q.VerboseLogging {
						log.Printf("[%s] exiting backoff. returning to RDY %d", c, count)
					}
					q.updateRDY(c, count)
				}
				continue
			}

			// start or continue backoff
			if backoffCounter > 0 {
				backoffDuration := q.backoffDurationForCount(backoffCounter)
				atomic.StoreInt64(&q.backoffDuration, backoffDuration.Nanoseconds())
				backoffTimer = time.NewTimer(backoffDuration)
				backoffTimerChan = backoffTimer.C

				log.Printf("backing off for %.04f seconds (backoff level %d)",
					backoffDuration.Seconds(), backoffCounter)

				// send RDY 0 immediately (to *all* connections)
				for _, c := range q.conns() {
					if q.VerboseLogging {
						log.Printf("[%s] in backoff. sending RDY 0", c)
					}
					q.updateRDY(c, 0)
				}
			}
		case <-redistributeTicker.C:
			q.redistributeRDY()
		case <-q.ExitChan:
			goto exit
		}
	}

exit:
	redistributeTicker.Stop()
	if backoffTimer != nil {
		backoffTimer.Stop()
	}
	log.Printf("rdyLoop exiting")
}

func (q *Reader) updateRDY(c *Conn, count int64) error {
	if c.IsStopping() {
		return nil
	}

	// never exceed the nsqd's configured max RDY count
	if count > c.MaxRDY() {
		count = c.MaxRDY()
	}

	// stop any pending retry of an old RDY update
	c.Lock()
	if timer, ok := q.rdyRetryTimers[c.String()]; ok {
		timer.Stop()
		delete(q.rdyRetryTimers, c.String())
	}
	c.Unlock()

	// never exceed our global max in flight. truncate if possible.
	// this could help a new connection get partial max-in-flight
	rdyCount := c.RDY()
	maxPossibleRdy := int64(q.MaxInFlight()) - atomic.LoadInt64(&q.totalRdyCount) + rdyCount
	if maxPossibleRdy > 0 && maxPossibleRdy < count {
		count = maxPossibleRdy
	}
	if maxPossibleRdy <= 0 && count > 0 {
		if rdyCount == 0 {
			// we wanted to exit a zero RDY count but we couldn't send it...
			// in order to prevent eternal starvation we reschedule this attempt
			// (if any other RDY update succeeds this timer will be stopped)
			c.Lock()
			q.rdyRetryTimers[c.String()] = time.AfterFunc(5*time.Second,
				func() {
					q.updateRDY(c, count)
				})
			c.Unlock()
		}
		return ErrOverMaxInFlight
	}

	return q.sendRDY(c, count)
}

func (q *Reader) sendRDY(c *Conn, count int64) error {
	if count == 0 && c.LastRDY() == 0 {
		// no need to send. It's already that RDY count
		return nil
	}

	atomic.AddInt64(&q.totalRdyCount, -c.RDY()+count)
	c.SetRDY(count)
	err := c.SendCommand(Ready(int(count)))
	if err != nil {
		log.Printf("[%s] error sending RDY %d - %s", c, count, err)
		c.Stop()
		return err
	}
	return nil
}

func (q *Reader) redistributeRDY() {
	if q.inBackoffBlock() {
		return
	}

	numConns := len(q.conns())
	maxInFlight := q.MaxInFlight()
	if numConns > maxInFlight {
		log.Printf("redistributing RDY state (%d conns > %d max_in_flight)",
			numConns, maxInFlight)
		atomic.StoreInt32(&q.needRDYRedistributed, 1)
	}

	if q.inBackoff() && numConns > 1 {
		log.Printf("redistributing RDY state (in backoff and %d conns > 1)", numConns)
		atomic.StoreInt32(&q.needRDYRedistributed, 1)
	}

	if !atomic.CompareAndSwapInt32(&q.needRDYRedistributed, 1, 0) {
		return
	}

	conns := q.conns()
	possibleConns := make([]*Conn, 0, len(conns))
	for _, c := range conns {
		lastMsgDuration := time.Now().Sub(c.LastMessageTime())
		rdyCount := c.RDY()
		if q.VerboseLogging {
			log.Printf("[%s] rdy: %d (last message received %s)",
				c, rdyCount, lastMsgDuration)
		}
		if rdyCount > 0 && lastMsgDuration > q.LowRdyIdleTimeout {
			log.Printf("[%s] idle connection, giving up RDY count", c)
			q.updateRDY(c, 0)
		}
		possibleConns = append(possibleConns, c)
	}

	availableMaxInFlight := int64(maxInFlight) - atomic.LoadInt64(&q.totalRdyCount)
	if q.inBackoff() {
		availableMaxInFlight = 1 - atomic.LoadInt64(&q.totalRdyCount)
	}

	for len(possibleConns) > 0 && availableMaxInFlight > 0 {
		availableMaxInFlight--
		i := rand.Int() % len(possibleConns)
		c := possibleConns[i]
		// delete
		possibleConns = append(possibleConns[:i], possibleConns[i+1:]...)
		log.Printf("[%s] redistributing RDY", c)
		q.updateRDY(c, 1)
	}
}

// Stop will gracefully stop the Reader
func (q *Reader) Stop() {
	if !atomic.CompareAndSwapInt32(&q.stopFlag, 0, 1) {
		return
	}

	log.Printf("stopping reader")

	if len(q.conns()) == 0 {
		q.stopHandlers()
	} else {
		for _, c := range q.conns() {
			err := c.SendCommand(StartClose())
			if err != nil {
				log.Printf("[%s] failed to start close - %s", c, err.Error())
				c.Stop()
			}
		}

		time.AfterFunc(time.Second*30, func() {
			q.stopHandlers()
		})
	}
}

func (q *Reader) stopHandlers() {
	q.stopHandler.Do(func() {
		log.Printf("stopping handlers")
		close(q.incomingMessages)
	})
}

// AddHandler adds a Handler for messages received by this Reader.
//
// See Handler for details on implementing this interface.
//
// It's ok to start more than one handler simultaneously, they
// are concurrently executed in goroutines.
func (q *Reader) AddHandler(handler Handler) {
	atomic.AddInt32(&q.runningHandlers, 1)
	go q.syncHandler(handler)
}

func (q *Reader) syncHandler(handler Handler) {
	log.Println("Handler starting")
	for {
		message, ok := <-q.incomingMessages
		if !ok {
			log.Printf("Handler closing")
			if atomic.AddInt32(&q.runningHandlers, -1) == 0 {
				close(q.ExitChan)
			}
			break
		}

		finishedMessage := q.checkMessageAttempts(message, handler)
		if finishedMessage != nil {
			message.responseChan <- finishedMessage
			continue
		}

		err := handler.HandleMessage(message)
		if err != nil {
			log.Printf("ERROR: handler returned %s for msg %s %s",
				err.Error(), message.Id, message.Body)
		}

		// linear delay
		requeueDelay := q.DefaultRequeueDelay * time.Duration(message.Attempts)
		// bound the requeueDelay to configured max
		if requeueDelay > q.MaxRequeueDelay {
			requeueDelay = q.MaxRequeueDelay
		}

		message.responseChan <- &FinishedMessage{
			Id:             message.Id,
			RequeueDelayMs: int(requeueDelay / time.Millisecond),
			Success:        err == nil,
		}
	}
}

// AddAsyncHandler adds an AsyncHandler for messages received by this Reader.
//
// See AsyncHandler for details on implementing this interface.
//
// It's ok to start more than one handler simultaneously, they
// are concurrently executed in goroutines.
func (q *Reader) AddAsyncHandler(handler AsyncHandler) {
	atomic.AddInt32(&q.runningHandlers, 1)
	go q.asyncHandler(handler)
}

func (q *Reader) asyncHandler(handler AsyncHandler) {
	log.Println("AsyncHandler starting")
	for {
		message, ok := <-q.incomingMessages
		if !ok {
			log.Printf("AsyncHandler closing")
			if atomic.AddInt32(&q.runningHandlers, -1) == 0 {
				close(q.ExitChan)
			}
			break
		}

		finishedMessage := q.checkMessageAttempts(message, handler)
		if finishedMessage != nil {
			message.responseChan <- finishedMessage
			continue
		}

		handler.HandleMessage(message, message.responseChan)
	}
}

func (q *Reader) checkMessageAttempts(message *Message, handler interface{}) *FinishedMessage {
	// message passed the max number of attempts
	if q.MaxAttemptCount > 0 && message.Attempts > q.MaxAttemptCount {
		log.Printf("WARNING: msg attempted %d times. giving up %s %s",
			message.Attempts, message.Id, message.Body)

		logger, ok := handler.(FailedMessageLogger)
		if ok {
			logger.LogFailedMessage(message)
		}

		return &FinishedMessage{message.Id, 0, true}
	}
	return nil
}
