package nsq

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// returned from ConnectToNSQ when already connected
var ErrAlreadyConnected = errors.New("already connected")

// returned from updateRdy if over max-in-flight
var ErrOverMaxInFlight = errors.New("over configure max-inflight")

// returned from ConnectToLookupd when given lookupd address exists already
var ErrLookupdAddressExists = errors.New("lookupd address already exists")

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
	messagesInFlight int64
	backoffDuration  int64

	sync.RWMutex

	// basics
	TopicName       string   // name of topic to subscribe to
	ChannelName     string   // name of channel to subscribe to
	ShortIdentifier string   // an identifier to send to nsqd when connecting (defaults: short hostname)
	LongIdentifier  string   // an identifier to send to nsqd when connecting (defaults: long hostname)
	VerboseLogging  bool     // enable verbose logging
	ExitChan        chan int // read from this channel to block your main loop

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

	// transport layer security
	TLSv1     bool        // negotiate enabling TLS
	TLSConfig *tls.Config // client TLS configuration

	// compression
	Deflate      bool // negotiate enabling Deflate compression
	DeflateLevel int  // the compression level to negotiate for Deflate
	Snappy       bool // negotiate enabling Snappy compression

	SampleRate int32  // set the sampleRate of the client's messagePump (requires nsqd 0.2.25+)
	UserAgent  string // a string identifying the agent for this client in the spirit of HTTP (default: "<client_library_name>/<version>")

	// internal variables
	maxBackoffDuration   time.Duration
	maxBackoffCount      int32
	backoffChan          chan bool
	rdyChan              chan *nsqConn
	needRDYRedistributed int32
	backoffCounter       int32

	maxInFlightMutex sync.RWMutex
	maxInFlight      int

	incomingMessages chan *Message

	pendingConnections map[string]bool
	nsqConnections     map[string]*nsqConn

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

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
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

		ShortIdentifier: strings.Split(hostname, ".")[0],
		LongIdentifier:  hostname,

		ReadTimeout:  DefaultClientTimeout,
		WriteTimeout: time.Second,

		DeflateLevel: 6,

		incomingMessages: make(chan *Message),

		pendingConnections: make(map[string]bool),
		nsqConnections:     make(map[string]*nsqConn),

		lookupdRecheckChan: make(chan int, 1), // used at connection close to force a possible reconnect
		maxInFlight:        1,
		backoffChan:        make(chan bool),
		rdyChan:            make(chan *nsqConn, 1),

		ExitChan: make(chan int),
	}
	q.SetMaxBackoffDuration(120 * time.Second)
	go q.rdyLoop()
	return q, nil
}

// Configure takes an option as a string and a value as an interface and
// attempts to set the appropriate configuration option on the reader instance.
//
// It attempts to coerce the value into the right format depending on the named
// option and the underlying type of the value passed in.
//
// It returns an error for an invalid option or value.
func (q *Reader) Configure(option string, value interface{}) error {
	getDuration := func(v interface{}) (time.Duration, error) {
		switch v.(type) {
		case string:
			return time.ParseDuration(v.(string))
		case int, int16, uint16, int32, uint32, int64, uint64:
			// treat like ms
			return time.Duration(reflect.ValueOf(v).Int()) * time.Millisecond, nil
		case time.Duration:
			return v.(time.Duration), nil
		}
		return 0, errors.New("invalid value type")
	}

	getBool := func(v interface{}) (bool, error) {
		switch value.(type) {
		case bool:
			return value.(bool), nil
		case string:
			return strconv.ParseBool(v.(string))
		case int, int16, uint16, int32, uint32, int64, uint64:
			return reflect.ValueOf(value).Int() == 0, nil
		}
		return false, errors.New("invalid value type")
	}

	getFloat64 := func(v interface{}) (float64, error) {
		switch value.(type) {
		case string:
			return strconv.ParseFloat(value.(string), 64)
		case int, int16, uint16, int32, uint32, int64, uint64:
			return float64(reflect.ValueOf(value).Int()), nil
		case float64:
			return value.(float64), nil
		}
		return 0, errors.New("invalid value type")
	}

	getInt64 := func(v interface{}) (int64, error) {
		switch value.(type) {
		case string:
			return strconv.ParseInt(v.(string), 10, 64)
		case int, int16, uint16, int32, uint32, int64, uint64:
			return reflect.ValueOf(value).Int(), nil
		}
		return 0, errors.New("invalid value type")
	}

	switch option {
	case "read_timeout":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 5*time.Minute || v < 100*time.Millisecond {
			return errors.New(fmt.Sprintf("invalid %s ! 100ms <= %s <= 5m", option, v))
		}
		q.ReadTimeout = v
	case "write_timeout":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 5*time.Minute || v < 100*time.Millisecond {
			return errors.New(fmt.Sprintf("invalid %s ! 100ms <= %s <= 5m", option, v))
		}
		q.WriteTimeout = v
	case "lookupd_poll_interval":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 5*time.Minute || v < 5*time.Second {
			return errors.New(fmt.Sprintf("invalid %s ! 5s <= %s <= 5m", option, v))
		}
		q.LookupdPollInterval = v
	case "lookupd_poll_jitter":
		v, err := getFloat64(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v < 0 || v > 1 {
			return errors.New(fmt.Sprintf("invalid %s ! 0 <= %d <= 1", option, v))
		}
		q.LookupdPollJitter = v
	case "max_requeue_delay":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 60*time.Minute || v < 0 {
			return errors.New(fmt.Sprintf("invalid %s ! 0 <= %s <= 60m", option, v))
		}
		q.MaxRequeueDelay = v
	case "default_requeue_delay":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 60*time.Minute || v < 0 {
			return errors.New(fmt.Sprintf("invalid %s ! 0 <= %s <= 60m", option, v))
		}
		q.DefaultRequeueDelay = v
	case "backoff_multiplier":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 60*time.Minute || v < time.Second {
			return errors.New(fmt.Sprintf("invalid %s ! 1s <= %s <= 60m", option, v))
		}
		q.BackoffMultiplier = v
	case "max_attempt_count":
		v, err := getInt64(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v < 1 || v > 65535 {
			return errors.New(fmt.Sprintf("invalid %s ! 1 <= %d <= 65535", option, v))
		}
		q.MaxAttemptCount = uint16(v)
	case "low_rdy_idle_timeout":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 5*time.Minute || v < time.Second {
			return errors.New(fmt.Sprintf("invalid %s ! 1s <= %s <= 5m", option, v))
		}
		q.LowRdyIdleTimeout = v
	case "tls_v1":
		v, err := getBool(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		q.TLSv1 = v
	case "deflate":
		v, err := getBool(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		q.Deflate = v
	case "deflate_level":
		v, err := getInt64(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v < 1 || v > 9 {
			return errors.New(fmt.Sprintf("invalid %s ! 1 <= %d <= 9", option, v))
		}
		q.DeflateLevel = int(v)
	case "sample_rate":
		v, err := getInt64(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v < 0 || v > 99 {
			return errors.New(fmt.Sprintf("invalid %s ! 0 <= %d <= 99", option, err))
		}
		q.SampleRate = int32(v)
	case "user_agent":
		q.UserAgent = value.(string)
	case "snappy":
		v, err := getBool(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		q.Snappy = v
	case "max_in_flight":
		v, err := getInt64(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v < 1 {
			return errors.New(fmt.Sprintf("invalid %s ! 1 <= %d", option, v))
		}
		q.SetMaxInFlight(int(v))
	case "max_backoff_duration":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 60*time.Minute || v < 0 {
			return errors.New(fmt.Sprintf("invalid %s ! 0 <= %s <= 60m", option, v))
		}
		q.SetMaxBackoffDuration(v)
	case "verbose":
		v, err := getBool(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		q.VerboseLogging = v
	}

	return nil
}

// ConnectionMaxInFlight calculates the per-connection max-in-flight count.
//
// This may change dynamically based on the number of connections to nsqd the Reader
// is responsible for.
func (q *Reader) ConnectionMaxInFlight() int64 {
	b := float64(q.MaxInFlight())
	q.RLock()
	s := b / float64(len(q.nsqConnections))
	q.RUnlock()
	return int64(math.Min(math.Max(1, s), b))
}

// IsStarved indicates whether any connections for this reader are blocked on processing
// before being able to receive more messages (ie. RDY count of 0 and not exiting)
func (q *Reader) IsStarved() bool {
	q.RLock()
	defer q.RUnlock()

	for _, conn := range q.nsqConnections {
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

	q.RLock()
	defer q.RUnlock()
	for _, c := range q.nsqConnections {
		c.rdyChan <- c
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
	var buf bytes.Buffer

	if atomic.LoadInt32(&q.stopFlag) == 1 {
		return errors.New("reader stopped")
	}

	if atomic.LoadInt32(&q.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	q.RLock()
	_, ok := q.nsqConnections[addr]
	_, pendingOk := q.pendingConnections[addr]
	if ok || pendingOk {
		q.RUnlock()
		return ErrAlreadyConnected
	}
	q.RUnlock()

	log.Printf("[%s] connecting to nsqd", addr)

	connection, err := newNSQConn(q.rdyChan, addr,
		q.TopicName, q.ChannelName, q.ReadTimeout, q.WriteTimeout)
	if err != nil {
		return err
	}
	cleanupConnection := func() {
		q.Lock()
		delete(q.pendingConnections, addr)
		q.Unlock()
		connection.Close()
	}
	q.pendingConnections[addr] = true

	// set the user_agent string to the default if there is no user input version
	userAgent := fmt.Sprintf("go-nsq/%s", VERSION)
	if q.UserAgent != "" {
		userAgent = q.UserAgent
	}

	ci := make(map[string]interface{})
	ci["short_id"] = q.ShortIdentifier
	ci["long_id"] = q.LongIdentifier
	ci["tls_v1"] = q.TLSv1
	ci["deflate"] = q.Deflate
	ci["deflate_level"] = q.DeflateLevel
	ci["snappy"] = q.Snappy
	ci["feature_negotiation"] = true
	ci["sample_rate"] = q.SampleRate
	ci["user_agent"] = userAgent
	cmd, err := Identify(ci)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to create identify command - %s", connection, err.Error())
	}

	err = connection.sendCommand(&buf, cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to identify - %s", connection, err.Error())
	}

	_, data, err := connection.readUnpackedResponse()
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] error reading response %s", connection, err.Error())
	}

	// check to see if the server was able to respond w/ capabilities
	if data[0] == '{' {
		resp := struct {
			MaxRdyCount int64 `json:"max_rdy_count"`
			TLSv1       bool  `json:"tls_v1"`
			Deflate     bool  `json:"deflate"`
			Snappy      bool  `json:"snappy"`
			SampleRate  int32 `json:"sample_rate"`
		}{}
		err := json.Unmarshal(data, &resp)
		if err != nil {
			cleanupConnection()
			return fmt.Errorf("[%s] error (%s) unmarshaling IDENTIFY response %s", connection, err.Error(), data)
		}

		log.Printf("[%s] IDENTIFY response: %+v", connection, resp)

		connection.maxRdyCount = resp.MaxRdyCount
		if resp.MaxRdyCount < int64(q.MaxInFlight()) {
			log.Printf("[%s] max RDY count %d < reader max in flight %d, truncation possible",
				connection, resp.MaxRdyCount, q.MaxInFlight())
		}

		if resp.TLSv1 {
			log.Printf("[%s] upgrading to TLS", connection)
			err := connection.upgradeTLS(q.TLSConfig)
			if err != nil {
				cleanupConnection()
				return fmt.Errorf("[%s] error (%s) upgrading to TLS", connection, err.Error())
			}
		}

		if resp.Deflate {
			log.Printf("[%s] upgrading to Deflate", connection)
			err := connection.upgradeDeflate(q.DeflateLevel)
			if err != nil {
				connection.Close()
				return fmt.Errorf("[%s] error (%s) upgrading to deflate", connection, err.Error())
			}
		}

		if resp.Snappy {
			log.Printf("[%s] upgrading to Snappy", connection)
			err := connection.upgradeSnappy()
			if err != nil {
				connection.Close()
				return fmt.Errorf("[%s] error (%s) upgrading to snappy", connection, err.Error())
			}
		}
	}

	cmd = Subscribe(q.TopicName, q.ChannelName)
	err = connection.sendCommand(&buf, cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to subscribe to %s:%s - %s", connection, q.TopicName, q.ChannelName, err.Error())
	}

	connection.enableReadBuffering()

	q.Lock()
	delete(q.pendingConnections, addr)
	q.nsqConnections[addr] = connection
	q.Unlock()

	// pre-emptive signal to existing connections to lower their RDY count
	q.RLock()
	for _, c := range q.nsqConnections {
		c.rdyChan <- c
	}
	q.RUnlock()

	connection.wg.Add(2)
	go q.readLoop(connection)
	go q.finishLoop(connection)

	return nil
}

func handleError(q *Reader, c *nsqConn, errMsg string) {
	log.Printf(errMsg)
	atomic.StoreInt32(&c.stopFlag, 1)

	q.RLock()
	numLookupd := len(q.lookupdHTTPAddrs)
	q.RUnlock()
	if numLookupd == 0 {
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

func (q *Reader) readLoop(c *nsqConn) {
	for {
		if atomic.LoadInt32(&c.stopFlag) == 1 || atomic.LoadInt32(&q.stopFlag) == 1 {
			// start the connection close
			if atomic.LoadInt64(&c.messagesInFlight) == 0 {
				q.stopFinishLoop(c)
			} else {
				log.Printf("[%s] delaying close, %d outstanding messages",
					c, c.messagesInFlight)
			}
			goto exit
		}

		frameType, data, err := c.readUnpackedResponse()
		if err != nil {
			handleError(q, c, fmt.Sprintf("[%s] error (%s) reading response %d %s",
				c, err.Error(), frameType, data))
			continue
		}

		switch frameType {
		case FrameTypeMessage:
			msg, err := DecodeMessage(data)
			msg.cmdChan = c.cmdChan
			msg.responseChan = c.finishedMessages

			if err != nil {
				handleError(q, c, fmt.Sprintf("[%s] error (%s) decoding message %s",
					c, err.Error(), data))
				continue
			}

			remain := atomic.AddInt64(&c.rdyCount, -1)
			atomic.AddInt64(&q.totalRdyCount, -1)
			atomic.AddUint64(&c.messagesReceived, 1)
			atomic.AddUint64(&q.MessagesReceived, 1)
			atomic.AddInt64(&c.messagesInFlight, 1)
			atomic.AddInt64(&q.messagesInFlight, 1)
			atomic.StoreInt64(&c.lastMsgTimestamp, time.Now().UnixNano())

			if q.VerboseLogging {
				log.Printf("[%s] (remain %d) FrameTypeMessage: %s - %s",
					c, remain, msg.Id, msg.Body)
			}

			q.incomingMessages <- msg
			c.rdyChan <- c
		case FrameTypeResponse:
			switch {
			case bytes.Equal(data, []byte("CLOSE_WAIT")):
				// server is ready for us to close (it ack'd our StartClose)
				// we can assume we will not receive any more messages over this channel
				// (but we can still write back responses)
				log.Printf("[%s] received ACK from nsqd - now in CLOSE_WAIT", c)
				atomic.StoreInt32(&c.stopFlag, 1)
			case bytes.Equal(data, []byte("_heartbeat_")):
				var buf bytes.Buffer
				log.Printf("[%s] heartbeat received", c)
				err := c.sendCommand(&buf, Nop())
				if err != nil {
					handleError(q, c, fmt.Sprintf("[%s] error sending NOP - %s",
						c, err.Error()))
					goto exit
				}
			}
		case FrameTypeError:
			log.Printf("[%s] error from nsqd %s", c, data)
		default:
			log.Printf("[%s] unknown message type %d", c, frameType)
		}
	}

exit:
	c.wg.Done()
	log.Printf("[%s] readLoop exiting", c)
}

func (q *Reader) finishLoop(c *nsqConn) {
	var buf bytes.Buffer

	for {
		select {
		case <-c.exitChan:
			log.Printf("[%s] breaking out of finish loop", c)
			// Indicate drainReady because we will not pull any more off finishedMessages
			close(c.drainReady)
			goto exit
		case cmd := <-c.cmdChan:
			err := c.sendCommand(&buf, cmd)
			if err != nil {
				log.Printf("[%s] error sending command %s - %s", c, cmd, err)
				q.stopFinishLoop(c)
				continue
			}
		case msg := <-c.finishedMessages:
			// Decrement this here so it is correct even if we can't respond to nsqd
			atomic.AddInt64(&q.messagesInFlight, -1)
			atomic.AddInt64(&c.messagesInFlight, -1)

			if msg.Success {
				if q.VerboseLogging {
					log.Printf("[%s] finishing %s", c, msg.Id)
				}

				err := c.sendCommand(&buf, Finish(msg.Id))
				if err != nil {
					log.Printf("[%s] error finishing %s - %s", c, msg.Id, err.Error())
					q.stopFinishLoop(c)
					continue
				}

				atomic.AddUint64(&c.messagesFinished, 1)
				atomic.AddUint64(&q.MessagesFinished, 1)
			} else {
				if q.VerboseLogging {
					log.Printf("[%s] requeuing %s", c, msg.Id)
				}

				err := c.sendCommand(&buf, Requeue(msg.Id, msg.RequeueDelayMs))
				if err != nil {
					log.Printf("[%s] error requeueing %s - %s", c, msg.Id, err.Error())
					q.stopFinishLoop(c)
					continue
				}

				atomic.AddUint64(&c.messagesRequeued, 1)
				atomic.AddUint64(&q.MessagesRequeued, 1)
			}

			q.backoffChan <- msg.Success

			if atomic.LoadInt64(&c.messagesInFlight) == 0 &&
				(atomic.LoadInt32(&c.stopFlag) == 1 || atomic.LoadInt32(&q.stopFlag) == 1) {
				q.stopFinishLoop(c)
				continue
			}
		}
	}

exit:
	c.wg.Done()
	log.Printf("[%s] finishLoop exiting", c)
}

func (q *Reader) stopFinishLoop(c *nsqConn) {
	c.stopper.Do(func() {
		log.Printf("[%s] beginning stopFinishLoop", c)
		close(c.exitChan)
		c.Close()
		go q.cleanupConnection(c)
	})
}

func (q *Reader) cleanupConnection(c *nsqConn) {
	go func() {
		<-c.drainReady
		ticker := time.NewTicker(100 * time.Millisecond)
		// finishLoop has exited, drain any remaining in flight messages
		for {
			// we're racing with readLoop which potentially has a message
			// for handling...
			//
			// infinitely loop until the connection's waitgroup is satisfied,
			// ensuring that both finishLoop and readLoop have exited, at which
			// point we can be guaranteed that messagesInFlight accurately
			// represents whatever is left... continue until 0.
			var msgsInFlight int64
			select {
			case <-c.finishedMessages:
				msgsInFlight = atomic.AddInt64(&c.messagesInFlight, -1)
			case <-ticker.C:
				msgsInFlight = atomic.LoadInt64(&c.messagesInFlight)
			}
			if msgsInFlight > 0 {
				log.Printf("[%s] draining... waiting for %d messages in flight", c, msgsInFlight)
				continue
			}
			log.Printf("[%s] done draining finishedMessages", c)
			ticker.Stop()
			return
		}
	}()

	// this blocks until finishLoop and readLoop have exited
	c.wg.Wait()

	// remove this connections RDY count from the reader's total
	rdyCount := atomic.LoadInt64(&c.rdyCount)
	atomic.AddInt64(&q.totalRdyCount, -rdyCount)

	c.Lock()
	hasRDYRetryTimer := c.rdyRetryTimer != nil
	if c.rdyRetryTimer != nil {
		// stop any pending retry of an old RDY update
		c.rdyRetryTimer.Stop()
		c.rdyRetryTimer = nil
	}
	c.Unlock()

	q.Lock()
	delete(q.nsqConnections, c.addr)
	left := len(q.nsqConnections)
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
			q.RLock()
			// pick a random connection to test the waters
			choice := rand.Intn(len(q.nsqConnections))
			i := 0
			for _, c := range q.nsqConnections {
				if i != choice {
					i++
					continue
				}
				log.Printf("[%s] backoff time expired, continuing with RDY 1...", c)
				// while in backoff only ever let 1 message at a time through
				q.updateRDY(c, 1)
			}
			q.RUnlock()
			backoffTimer = nil
			backoffTimerChan = nil
			atomic.StoreInt64(&q.backoffDuration, 0)
		case c := <-q.rdyChan:
			if backoffTimer != nil || backoffCounter > 0 {
				continue
			}

			// send ready immediately
			remain := atomic.LoadInt64(&c.rdyCount)
			lastRdyCount := atomic.LoadInt64(&c.lastRdyCount)
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
				q.RLock()
				for _, c := range q.nsqConnections {
					if q.VerboseLogging {
						log.Printf("[%s] exiting backoff. returning to RDY %d", c, count)
					}
					q.updateRDY(c, count)
				}
				q.RUnlock()
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
				q.RLock()
				for _, c := range q.nsqConnections {
					if q.VerboseLogging {
						log.Printf("[%s] in backoff. sending RDY 0", c)
					}
					q.updateRDY(c, 0)
				}
				q.RUnlock()
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

func (q *Reader) updateRDY(c *nsqConn, count int64) error {
	if atomic.LoadInt32(&c.stopFlag) != 0 {
		return nil
	}

	// never exceed the nsqd's configured max RDY count
	if count > c.maxRdyCount {
		count = c.maxRdyCount
	}

	// stop any pending retry of an old RDY update
	c.Lock()
	if c.rdyRetryTimer != nil {
		c.rdyRetryTimer.Stop()
		c.rdyRetryTimer = nil
	}
	c.Unlock()

	// never exceed our global max in flight. truncate if possible.
	// this could help a new connection get partial max-in-flight
	rdyCount := atomic.LoadInt64(&c.rdyCount)
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
			c.rdyRetryTimer = time.AfterFunc(5*time.Second, func() {
				q.updateRDY(c, count)
			})
			c.Unlock()
		}
		return ErrOverMaxInFlight
	}

	return q.sendRDY(c, count)
}

func (q *Reader) sendRDY(c *nsqConn, count int64) error {
	var buf bytes.Buffer

	if count == 0 && atomic.LoadInt64(&c.lastRdyCount) == 0 {
		// no need to send. It's already that RDY count
		return nil
	}

	atomic.AddInt64(&q.totalRdyCount, -atomic.LoadInt64(&c.rdyCount)+count)
	atomic.StoreInt64(&c.rdyCount, count)
	atomic.StoreInt64(&c.lastRdyCount, count)
	err := c.sendCommand(&buf, Ready(int(count)))
	if err != nil {
		handleError(q, c, fmt.Sprintf("[%s] error sending RDY %d - %s",
			c, count, err.Error()))
		return err
	}
	return nil
}

func (q *Reader) redistributeRDY() {
	if q.inBackoffBlock() {
		return
	}

	q.RLock()
	numConns := len(q.nsqConnections)
	q.RUnlock()
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

	q.RLock()
	possibleConns := make([]*nsqConn, 0, len(q.nsqConnections))
	for _, c := range q.nsqConnections {
		lastMsgTimestamp := atomic.LoadInt64(&c.lastMsgTimestamp)
		lastMsgDuration := time.Now().Sub(time.Unix(0, lastMsgTimestamp))
		rdyCount := atomic.LoadInt64(&c.rdyCount)
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
	q.RUnlock()
}

// Stop will gracefully stop the Reader
func (q *Reader) Stop() {
	var buf bytes.Buffer

	if !atomic.CompareAndSwapInt32(&q.stopFlag, 0, 1) {
		return
	}

	log.Printf("stopping reader")

	q.RLock()
	l := len(q.nsqConnections)
	q.RUnlock()

	if l == 0 {
		q.stopHandlers()
	} else {
		q.RLock()
		for _, c := range q.nsqConnections {
			err := c.sendCommand(&buf, StartClose())
			if err != nil {
				log.Printf("[%s] failed to start close - %s", c, err.Error())
			}
		}
		q.RUnlock()

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
