package nsq

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Handler is the message processing interface for Consumer
//
// Implement this interface for handlers that return whether or not message
// processing completed successfully.
//
// When the return value is nil Consumer will automatically handle FINishing.
//
// When the returned value is non-nil Consumer will automatically handle REQueing.
type Handler interface {
	HandleMessage(message *Message) error
}

// HandlerFunc is a convenience type to avoid having to declare a struct
// to implement the Handler interface, it can be used like this:
//
// 	consumec.AddHandler(nsq.HandlerFunc(func(m *Message) error {
// 		// handle the message
// 	}))
type HandlerFunc func(message *Message) error

// HandleMessage implements the Handler interface
func (h HandlerFunc) HandleMessage(m *Message) error {
	return h(m)
}

// DiscoveryFilter is an interface accepted by `SetBehaviorDelegate()`
// for filtering the nsqds returned from discovery via nsqlookupd
type DiscoveryFilter interface {
	Filter([]string) []string
}

// FailedMessageLogger is an interface that can be implemented by handlers that wish
// to receive a callback when a message is deemed "failed" (i.e. the number of attempts
// exceeded the Consumer specified MaxAttemptCount)
type FailedMessageLogger interface {
	LogFailedMessage(message *Message)
}

// ConsumerStats represents a snapshot of the state of a Consumer's connections and the messages
// it has seen
type ConsumerStats struct {
	MessagesReceived uint64
	MessagesFinished uint64
	MessagesRequeued uint64
	Connections      int
}

var instCount int64

type backoffSignal int

const (
	backoffFlag backoffSignal = iota
	continueFlag
	resumeFlag
)

type Consumer interface {
	connEventsHandler
	connEventsMessageHandler

	SetLogger(l logger, lvl LogLevel)
	SetLoggerLevel(lvl LogLevel)
	SetLoggerForLevel(l logger, lvl LogLevel)

	ConnectToNSQLookupd(addr string) error
	ConnectToNSQLookupds(addresses []string) error
	ConnectToNSQDs(addresses []string) error
	ConnectToNSQD(addr string) error
	DisconnectFromNSQD(addr string) error
	DisconnectFromNSQLookupd(addr string) error

	AddHandler(handler Handler)
	AddConcurrentHandlers(handler Handler, concurrency int)

	Stats() *ConsumerStats
	SetBehaviorDelegate(cb interface{})

	IsStarved() bool
	ChangeMaxInFlight(maxInFlight int)
	conns() []Conn
	Stop()
	GetStopChan() chan int
}

// Consumer is a high-level type to consume from NSQ.
//
// A Consumer instance is supplied a Handler that will be executed
// concurrently via goroutines to handle processing the stream of messages
// consumed from the specified topic/channel. See: Handler/HandlerFunc
// for details on implementing the interface to create handlers.
//
// If configured, it will poll nsqlookupd instances and handle connection (and
// reconnection) to any discovered nsqds.
type nsqConsumer struct {
	Consumer

	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messagesReceived uint64
	messagesFinished uint64
	messagesRequeued uint64
	totalRdyCount    int64
	backoffDuration  int64
	backoffCounter   int32
	maxInFlight      int32

	mtx sync.RWMutex

	loggerCarrier LoggerCarrier

	behaviorDelegate interface{}

	id      int64
	topic   string
	channel string
	config  Config

	rngMtx sync.Mutex
	rng    *rand.Rand

	needRDYRedistributed int32

	backoffMtx sync.Mutex

	incomingMessages chan *Message

	rdyRetryMtx    sync.Mutex
	rdyRetryTimers map[string]*time.Timer

	pendingConnections map[string]Conn
	connections        map[string]Conn

	nsqdTCPAddrs []string

	// used at connection close to force a possible reconnect
	lookupdRecheckChan chan int
	lookupdHTTPAddrs   []string
	lookupdQueryIndex  int

	wg              sync.WaitGroup
	runningHandlers int32
	stopFlag        int32
	connectedFlag   int32
	stopHandler     sync.Once
	exitHandler     sync.Once

	// read from this channel to block until consumer is cleanly stopped
	StopChan chan int
	exitChan chan int
}

// NewConsumer creates a new instance of Consumer for the specified topic/channel
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewConsumer the values are no longer mutable (they are copied).
func NewConsumer(topic string, channel string, config *Config) (Consumer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if !IsValidTopicName(topic) {
		return nil, errors.New("invalid topic name")
	}

	if !IsValidChannelName(channel) {
		return nil, errors.New("invalid channel name")
	}

	c := &nsqConsumer{
		id: atomic.AddInt64(&instCount, 1),

		topic:   topic,
		channel: channel,
		config:  *config,

		loggerCarrier: newDefaultLoggerCarrier(),
		maxInFlight:   int32(config.MaxInFlight),

		incomingMessages: make(chan *Message),

		rdyRetryTimers:     make(map[string]*time.Timer),
		pendingConnections: make(map[string]Conn),
		connections:        make(map[string]Conn),

		lookupdRecheckChan: make(chan int, 1),

		rng: rand.New(rand.NewSource(time.Now().UnixNano())),

		StopChan: make(chan int),
		exitChan: make(chan int),
	}

	c.wg.Add(1)
	go c.rdyLoop()
	return c, nil
}

// Stats retrieves the current connection and message statistics for a Consumer
func (c *nsqConsumer) Stats() *ConsumerStats {
	return &ConsumerStats{
		MessagesReceived: atomic.LoadUint64(&c.messagesReceived),
		MessagesFinished: atomic.LoadUint64(&c.messagesFinished),
		MessagesRequeued: atomic.LoadUint64(&c.messagesRequeued),
		Connections:      len(c.conns()),
	}
}

func (c *nsqConsumer) conns() []Conn {
	c.mtx.RLock()
	conns := make([]Conn, 0, len(c.connections))
	for _, c := range c.connections {
		conns = append(conns, c)
	}
	c.mtx.RUnlock()
	return conns
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (c *nsqConsumer) SetLogger(l logger, lvl LogLevel) {
	c.loggerCarrier.SetLogger(l, lvl, "")
}

// SetLoggerForLevel assigns the same logger for specified `level`.
func (c *nsqConsumer) SetLoggerForLevel(l logger, lvl LogLevel) {
	c.loggerCarrier.SetLoggerForLevel(l, lvl, "")
}

// SetLoggerLevel sets the package logging level.
func (c *nsqConsumer) SetLoggerLevel(lvl LogLevel) {
	c.loggerCarrier.SetLoggerLevel(lvl)
}

// SetBehaviorDelegate takes a type implementing one or more
// of the following interfaces that modify the behavior
// of the `Consumer`:
//
//    DiscoveryFilter
//
func (c *nsqConsumer) SetBehaviorDelegate(cb interface{}) {
	matched := false

	if _, ok := cb.(DiscoveryFilter); ok {
		matched = true
	}

	if !matched {
		panic("behavior delegate does not have any recognized methods")
	}

	c.behaviorDelegate = cb
}

// perConnMaxInFlight calculates the per-connection max-in-flight count.
//
// This may change dynamically based on the number of connections to nsqd the Consumer
// is responsible foc.
func (c *nsqConsumer) perConnMaxInFlight() int64 {
	b := float64(c.getMaxInFlight())
	s := b / float64(len(c.conns()))
	return int64(math.Min(math.Max(1, s), b))
}

// IsStarved indicates whether any connections for this consumer are blocked on processing
// before being able to receive more messages (ie. RDY count of 0 and not exiting)
func (c *nsqConsumer) IsStarved() bool {
	for _, conn := range c.conns() {
		threshold := int64(float64(conn.RDY()) * 0.85)
		inFlight := atomic.LoadInt64(conn.getInflightMessageCount())
		if inFlight >= threshold && inFlight > 0 && !conn.IsClosing() {
			return true
		}
	}
	return false
}

func (c *nsqConsumer) getMaxInFlight() int32 {
	return atomic.LoadInt32(&c.maxInFlight)
}

// ChangeMaxInFlight sets a new maximum number of messages this comsumer instance
// will allow in-flight, and updates all existing connections as appropriate.
//
// For example, ChangeMaxInFlight(0) would pause message flow
//
// If already connected, it updates the reader RDY state for each connection.
func (c *nsqConsumer) ChangeMaxInFlight(maxInFlight int) {
	if c.getMaxInFlight() == int32(maxInFlight) {
		return
	}

	atomic.StoreInt32(&c.maxInFlight, int32(maxInFlight))

	for _, conn := range c.conns() {
		c.maybeUpdateRDY(conn)
	}
}

// ConnectToNSQLookupd adds an nsqlookupd address to the list for this Consumer instance.
//
// If it is the first to be added, it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (c *nsqConsumer) ConnectToNSQLookupd(addr string) error {
	if atomic.LoadInt32(&c.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}
	if atomic.LoadInt32(&c.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	if err := validatedLookupAddr(addr); err != nil {
		return err
	}

	atomic.StoreInt32(&c.connectedFlag, 1)

	c.mtx.Lock()
	for _, x := range c.lookupdHTTPAddrs {
		if x == addr {
			c.mtx.Unlock()
			return nil
		}
	}
	c.lookupdHTTPAddrs = append(c.lookupdHTTPAddrs, addr)
	numLookupd := len(c.lookupdHTTPAddrs)
	c.mtx.Unlock()

	// if this is the first one, kick off the go loop
	if numLookupd == 1 {
		c.queryLookupd()
		c.wg.Add(1)
		go c.lookupdLoop()
	}

	return nil
}

// ConnectToNSQLookupds adds multiple nsqlookupd address to the list for this Consumer instance.
//
// If adding the first address it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (c *nsqConsumer) ConnectToNSQLookupds(addresses []string) error {
	for _, addr := range addresses {
		err := c.ConnectToNSQLookupd(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

func validatedLookupAddr(addr string) error {
	if strings.Contains(addr, "/") {
		_, err := url.Parse(addr)
		if err != nil {
			return err
		}
		return nil
	}
	if !strings.Contains(addr, ":") {
		return errors.New("missing port")
	}
	return nil
}

// poll all known lookup servers every LookupdPollInterval
func (c *nsqConsumer) lookupdLoop() {
	// add some jitter so that multiple consumers discovering the same topic,
	// when restarted at the same time, dont all connect at once.
	c.rngMtx.Lock()
	jitter := time.Duration(int64(c.rng.Float64() *
		c.config.LookupdPollJitter * float64(c.config.LookupdPollInterval)))
	c.rngMtx.Unlock()
	var ticker *time.Ticker

	select {
	case <-time.After(jitter):
	case <-c.exitChan:
		goto exit
	}

	ticker = time.NewTicker(c.config.LookupdPollInterval)

	for {
		select {
		case <-ticker.C:
			c.queryLookupd()
		case <-c.lookupdRecheckChan:
			c.queryLookupd()
		case <-c.exitChan:
			goto exit
		}
	}

exit:
	if ticker != nil {
		ticker.Stop()
	}
	c.log(LogLevelInfo, "exiting lookupdLoop")
	c.wg.Done()
}

// return the next lookupd endpoint to query
// keeping track of which one was last used
func (c *nsqConsumer) nextLookupdEndpoint() string {
	c.mtx.RLock()
	if c.lookupdQueryIndex >= len(c.lookupdHTTPAddrs) {
		c.lookupdQueryIndex = 0
	}
	addr := c.lookupdHTTPAddrs[c.lookupdQueryIndex]
	num := len(c.lookupdHTTPAddrs)
	c.mtx.RUnlock()
	c.lookupdQueryIndex = (c.lookupdQueryIndex + 1) % num

	urlString := addr
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + addr
	}

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	if u.Path == "/" || u.Path == "" {
		u.Path = "/lookup"
	}

	v, err := url.ParseQuery(u.RawQuery)
	v.Add("topic", c.topic)
	u.RawQuery = v.Encode()
	return u.String()
}

type lookupResp struct {
	Channels  []string    `json:"channels"`
	Producers []*peerInfo `json:"producers"`
	Timestamp int64       `json:"timestamp"`
}

type peerInfo struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

// make an HTTP req to one of the configured nsqlookupd instances to discover
// which nsqd's provide the topic we are consuming.
//
// initiate a connection to any new producers that are identified.
func (c *nsqConsumer) queryLookupd() {
	retries := 0

retry:
	endpoint := c.nextLookupdEndpoint()

	c.log(LogLevelInfo, "querying nsqlookupd %s", endpoint)

	var data lookupResp
	err := apiRequestNegotiateV1("GET", endpoint, nil, &data)
	if err != nil {
		c.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
		retries++
		if retries < 3 {
			c.log(LogLevelInfo, "retrying with next nsqlookupd")
			goto retry
		}
		return
	}

	var nsqdAddrs []string
	for _, producer := range data.Producers {
		broadcastAddress := producer.BroadcastAddress
		port := producer.TCPPort
		joined := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		nsqdAddrs = append(nsqdAddrs, joined)
	}
	// apply filter
	if discoveryFilter, ok := c.behaviorDelegate.(DiscoveryFilter); ok {
		nsqdAddrs = discoveryFilter.Filter(nsqdAddrs)
	}
	for _, addr := range nsqdAddrs {
		err = c.ConnectToNSQD(addr)
		if err != nil && err != ErrAlreadyConnected {
			c.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
			continue
		}
	}
}

// ConnectToNSQDs takes multiple nsqd addresses to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to local instance.
func (c *nsqConsumer) ConnectToNSQDs(addresses []string) error {
	for _, addr := range addresses {
		err := c.ConnectToNSQD(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

// ConnectToNSQD takes a nsqd address to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to a single, local,
// instance.
func (c *nsqConsumer) ConnectToNSQD(addr string) error {
	if atomic.LoadInt32(&c.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}

	if atomic.LoadInt32(&c.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	atomic.StoreInt32(&c.connectedFlag, 1)

	conn := NewConn(addr, &c.config, &consumerConnDelegate{c})
	conn.SetLoggerLevel(c.loggerCarrier.GetLogLevel())
	format := fmt.Sprintf("%3d [%s/%s] (%%s)", c.id, c.topic, c.channel)
	for index, logger := range c.loggerCarrier.GetLoggers() {
		conn.SetLoggerForLevel(logger, LogLevel(index), format)
	}
	c.mtx.Lock()
	_, pendingOk := c.pendingConnections[addr]
	_, ok := c.connections[addr]
	if ok || pendingOk {
		c.mtx.Unlock()
		return ErrAlreadyConnected
	}
	c.pendingConnections[addr] = conn
	if idx := indexOf(addr, c.nsqdTCPAddrs); idx == -1 {
		c.nsqdTCPAddrs = append(c.nsqdTCPAddrs, addr)
	}
	c.mtx.Unlock()

	c.log(LogLevelInfo, "(%s) connecting to nsqd", addr)

	cleanupConnection := func() {
		c.mtx.Lock()
		delete(c.pendingConnections, addr)
		c.mtx.Unlock()
		conn.Close()
	}

	resp, err := conn.Connect()
	if err != nil {
		cleanupConnection()
		return err
	}

	if resp != nil {
		if resp.MaxRdyCount < int64(c.getMaxInFlight()) {
			c.log(LogLevelWarning,
				"(%s) max RDY count %d < consumer max in flight %d, truncation possible",
				conn.String(), resp.MaxRdyCount, c.getMaxInFlight())
		}
	}

	cmd := Subscribe(c.topic, c.channel)
	err = conn.WriteCommand(cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to subscribe to %s:%s - %s",
			conn, c.topic, c.channel, err.Error())
	}

	c.mtx.Lock()
	delete(c.pendingConnections, addr)
	c.connections[addr] = conn
	c.mtx.Unlock()

	// pre-emptive signal to existing connections to lower their RDY count
	for _, conn := range c.conns() {
		c.maybeUpdateRDY(conn)
	}

	return nil
}

func indexOf(n string, h []string) int {
	for i, a := range h {
		if n == a {
			return i
		}
	}
	return -1
}

// DisconnectFromNSQD closes the connection to and removes the specified
// `nsqd` address from the list
func (c *nsqConsumer) DisconnectFromNSQD(addr string) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	idx := indexOf(addr, c.nsqdTCPAddrs)
	if idx == -1 {
		return ErrNotConnected
	}

	// slice delete
	c.nsqdTCPAddrs = append(c.nsqdTCPAddrs[:idx], c.nsqdTCPAddrs[idx+1:]...)

	pendingConn, pendingOk := c.pendingConnections[addr]
	conn, ok := c.connections[addr]

	if ok {
		conn.Close()
	} else if pendingOk {
		pendingConn.Close()
	}

	return nil
}

// DisconnectFromNSQLookupd removes the specified `nsqlookupd` address
// from the list used for periodic discovery.
func (c *nsqConsumer) DisconnectFromNSQLookupd(addr string) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	idx := indexOf(addr, c.lookupdHTTPAddrs)
	if idx == -1 {
		return ErrNotConnected
	}

	if len(c.lookupdHTTPAddrs) == 1 {
		return fmt.Errorf("cannot disconnect from only remaining nsqlookupd HTTP address %s", addr)
	}

	c.lookupdHTTPAddrs = append(c.lookupdHTTPAddrs[:idx], c.lookupdHTTPAddrs[idx+1:]...)

	return nil
}

func (c *nsqConsumer) onConnMessage(conn Conn, msg *Message) {
	atomic.AddUint64(&c.messagesReceived, 1)
	c.incomingMessages <- msg
}

func (c *nsqConsumer) onConnMessageFinished(conn Conn, msg *Message) {
	atomic.AddUint64(&c.messagesFinished, 1)
}

func (c *nsqConsumer) onConnMessageRequeued(conn Conn, msg *Message) {
	atomic.AddUint64(&c.messagesRequeued, 1)
}

func (c *nsqConsumer) onConnBackoff(conn Conn) {
	c.startStopContinueBackoff(conn, backoffFlag)
}

func (c *nsqConsumer) onConnContinue(conn Conn) {
	c.startStopContinueBackoff(conn, continueFlag)
}

func (c *nsqConsumer) onConnResume(conn Conn) {
	c.startStopContinueBackoff(conn, resumeFlag)
}

func (c *nsqConsumer) onConnResponse(conn Conn, data []byte) {
	switch {
	case bytes.Equal(data, []byte("CLOSE_WAIT")):
		// server is ready for us to close (it ack'd our StartClose)
		// we can assume we will not receive any more messages over this channel
		// (but we can still write back responses)
		c.log(LogLevelInfo, "(%s) received CLOSE_WAIT from nsqd", conn.String())
		conn.Close()
	}
}

func (c *nsqConsumer) onConnError(conn Conn, data []byte) {}

func (c *nsqConsumer) onConnHeartbeat(conn Conn) {}

func (c *nsqConsumer) onConnIOError(conn Conn, err error) {
	conn.Close()
}

func (c *nsqConsumer) onConnClose(conn Conn) {
	var hasRDYRetryTimer bool

	// remove this connections RDY count from the consumer's total
	rdyCount := conn.RDY()
	atomic.AddInt64(&c.totalRdyCount, -rdyCount)

	c.rdyRetryMtx.Lock()
	if timer, ok := c.rdyRetryTimers[conn.String()]; ok {
		// stop any pending retry of an old RDY update
		timer.Stop()
		delete(c.rdyRetryTimers, conn.String())
		hasRDYRetryTimer = true
	}
	c.rdyRetryMtx.Unlock()

	c.mtx.Lock()
	delete(c.connections, conn.String())
	left := len(c.connections)
	c.mtx.Unlock()

	c.log(LogLevelWarning, "there are %d connections left alive", left)

	if (hasRDYRetryTimer || rdyCount > 0) &&
		(int32(left) == c.getMaxInFlight() || c.inBackoff()) {
		// we're toggling out of (normal) redistribution cases and this conn
		// had a RDY count...
		//
		// trigger RDY redistribution to make sure this RDY is moved
		// to a new connection
		atomic.StoreInt32(&c.needRDYRedistributed, 1)
	}

	// we were the last one (and stopping)
	if atomic.LoadInt32(&c.stopFlag) == 1 {
		if left == 0 {
			c.stopHandlers()
		}
		return
	}

	c.mtx.RLock()
	numLookupd := len(c.lookupdHTTPAddrs)
	reconnect := indexOf(conn.String(), c.nsqdTCPAddrs) >= 0
	c.mtx.RUnlock()
	if numLookupd > 0 {
		// trigger a poll of the lookupd
		select {
		case c.lookupdRecheckChan <- 1:
		default:
		}
	} else if reconnect {
		// there are no lookupd and we still have this nsqd TCP address in our list...
		// try to reconnect after a bit
		go func(addr string) {
			for {
				c.log(LogLevelInfo, "(%s) re-connecting in %s", addr, c.config.LookupdPollInterval)
				time.Sleep(c.config.LookupdPollInterval)
				if atomic.LoadInt32(&c.stopFlag) == 1 {
					break
				}
				c.mtx.RLock()
				reconnect := indexOf(addr, c.nsqdTCPAddrs) >= 0
				c.mtx.RUnlock()
				if !reconnect {
					c.log(LogLevelWarning, "(%s) skipped reconnect after removal...", addr)
					return
				}
				err := c.ConnectToNSQD(addr)
				if err != nil && err != ErrAlreadyConnected {
					c.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
					continue
				}
				break
			}
		}(conn.String())
	}
}

func (c *nsqConsumer) startStopContinueBackoff(conn Conn, signal backoffSignal) {
	// prevent many async failures/successes from immediately resulting in
	// max backoff/normal rate (by ensuring that we dont continually incr/decr
	// the counter during a backoff period)
	c.backoffMtx.Lock()
	defer c.backoffMtx.Unlock()
	if c.inBackoffTimeout() {
		return
	}

	// update backoff state
	backoffUpdated := false
	backoffCounter := atomic.LoadInt32(&c.backoffCounter)
	switch signal {
	case resumeFlag:
		if backoffCounter > 0 {
			backoffCounter--
			backoffUpdated = true
		}
	case backoffFlag:
		nextBackoff := c.config.BackoffStrategy.Calculate(int(backoffCounter) + 1)
		if nextBackoff <= c.config.MaxBackoffDuration {
			backoffCounter++
			backoffUpdated = true
		}
	}
	atomic.StoreInt32(&c.backoffCounter, backoffCounter)

	if c.backoffCounter == 0 && backoffUpdated {
		// exit backoff
		count := c.perConnMaxInFlight()
		c.log(LogLevelWarning, "exiting backoff, returning all to RDY %d", count)
		for _, conn := range c.conns() {
			c.updateRDY(conn, count)
		}
	} else if c.backoffCounter > 0 {
		// start or continue backoff
		backoffDuration := c.config.BackoffStrategy.Calculate(int(backoffCounter))

		if backoffDuration > c.config.MaxBackoffDuration {
			backoffDuration = c.config.MaxBackoffDuration
		}

		c.log(LogLevelWarning, "backing off for %s (backoff level %d), setting all to RDY 0",
			backoffDuration, backoffCounter)

		// send RDY 0 immediately (to *all* connections)
		for _, conn := range c.conns() {
			c.updateRDY(conn, 0)
		}

		c.backoff(backoffDuration)
	}
}

func (c *nsqConsumer) backoff(d time.Duration) {
	atomic.StoreInt64(&c.backoffDuration, d.Nanoseconds())
	time.AfterFunc(d, c.resume)
}

func (c *nsqConsumer) resume() {
	if atomic.LoadInt32(&c.stopFlag) == 1 {
		atomic.StoreInt64(&c.backoffDuration, 0)
		return
	}

	// pick a random connection to test the waters
	conns := c.conns()
	if len(conns) == 0 {
		c.log(LogLevelWarning, "no connection available to resume")
		c.log(LogLevelWarning, "backing off for %s", time.Second)
		c.backoff(time.Second)
		return
	}
	c.rngMtx.Lock()
	idx := c.rng.Intn(len(conns))
	c.rngMtx.Unlock()
	choice := conns[idx]

	c.log(LogLevelWarning,
		"(%s) backoff timeout expired, sending RDY 1",
		choice.String())

	// while in backoff only ever let 1 message at a time through
	err := c.updateRDY(choice, 1)
	if err != nil {
		c.log(LogLevelWarning, "(%s) error resuming RDY 1 - %s", choice.String(), err)
		c.log(LogLevelWarning, "backing off for %s", time.Second)
		c.backoff(time.Second)
		return
	}

	atomic.StoreInt64(&c.backoffDuration, 0)
}

func (c *nsqConsumer) inBackoff() bool {
	return atomic.LoadInt32(&c.backoffCounter) > 0
}

func (c *nsqConsumer) inBackoffTimeout() bool {
	return atomic.LoadInt64(&c.backoffDuration) > 0
}

func (c *nsqConsumer) maybeUpdateRDY(conn Conn) {
	inBackoff := c.inBackoff()
	inBackoffTimeout := c.inBackoffTimeout()
	if inBackoff || inBackoffTimeout {
		c.log(LogLevelDebug, "(%s) skip sending RDY inBackoff:%v || inBackoffTimeout:%v",
			conn, inBackoff, inBackoffTimeout)
		return
	}

	count := c.perConnMaxInFlight()
	c.log(LogLevelDebug, "(%s) sending RDY %d", conn, count)
	c.updateRDY(conn, count)
}

func (c *nsqConsumer) rdyLoop() {
	redistributeTicker := time.NewTicker(c.config.RDYRedistributeInterval)

	for {
		select {
		case <-redistributeTicker.C:
			c.redistributeRDY()
		case <-c.exitChan:
			goto exit
		}
	}

exit:
	redistributeTicker.Stop()
	c.log(LogLevelInfo, "rdyLoop exiting")
	c.wg.Done()
}

func (c *nsqConsumer) updateRDY(conn Conn, count int64) error {
	if conn.IsClosing() {
		return ErrClosing
	}

	// never exceed the nsqd's configured max RDY count
	if count > conn.MaxRDY() {
		count = conn.MaxRDY()
	}

	// stop any pending retry of an old RDY update
	c.rdyRetryMtx.Lock()
	if timer, ok := c.rdyRetryTimers[conn.String()]; ok {
		timer.Stop()
		delete(c.rdyRetryTimers, conn.String())
	}
	c.rdyRetryMtx.Unlock()

	// never exceed our global max in flight. truncate if possible.
	// this could help a new connection get partial max-in-flight
	rdyCount := conn.RDY()
	maxPossibleRdy := int64(c.getMaxInFlight()) - atomic.LoadInt64(&c.totalRdyCount) + rdyCount
	if maxPossibleRdy > 0 && maxPossibleRdy < count {
		count = maxPossibleRdy
	}
	if maxPossibleRdy <= 0 && count > 0 {
		if rdyCount == 0 {
			// we wanted to exit a zero RDY count but we couldn't send it...
			// in order to prevent eternal starvation we reschedule this attempt
			// (if any other RDY update succeeds this timer will be stopped)
			c.rdyRetryMtx.Lock()
			c.rdyRetryTimers[conn.String()] = time.AfterFunc(5*time.Second,
				func() {
					c.updateRDY(conn, count)
				})
			c.rdyRetryMtx.Unlock()
		}
		return ErrOverMaxInFlight
	}

	return c.sendRDY(conn, count)
}

func (c *nsqConsumer) sendRDY(conn Conn, count int64) error {
	if count == 0 && conn.LastRDY() == 0 {
		// no need to send. It's already that RDY count
		return nil
	}

	atomic.AddInt64(&c.totalRdyCount, count-conn.RDY())

	lastRDY := conn.LastRDY()
	conn.SetRDY(count)
	if count == lastRDY {
		return nil
	}

	err := conn.WriteCommand(Ready(int(count)))
	if err != nil {
		c.log(LogLevelError, "(%s) error sending RDY %d - %s", conn.String(), count, err)
		return err
	}
	return nil
}

func (c *nsqConsumer) redistributeRDY() {
	if c.inBackoffTimeout() {
		return
	}

	// if an external heuristic set needRDYRedistributed we want to wait
	// until we can actually redistribute to proceed
	conns := c.conns()
	if len(conns) == 0 {
		return
	}

	maxInFlight := c.getMaxInFlight()
	if len(conns) > int(maxInFlight) {
		c.log(LogLevelDebug, "redistributing RDY state (%d conns > %d max_in_flight)",
			len(conns), maxInFlight)
		atomic.StoreInt32(&c.needRDYRedistributed, 1)
	}

	if c.inBackoff() && len(conns) > 1 {
		c.log(LogLevelDebug, "redistributing RDY state (in backoff and %d conns > 1)", len(conns))
		atomic.StoreInt32(&c.needRDYRedistributed, 1)
	}

	if !atomic.CompareAndSwapInt32(&c.needRDYRedistributed, 1, 0) {
		return
	}

	possibleConns := make([]Conn, 0, len(conns))
	for _, conn := range conns {
		lastMsgDuration := time.Now().Sub(conn.LastMessageTime())
		lastRdyDuration := time.Now().Sub(conn.LastRdyTime())
		rdyCount := conn.RDY()
		c.log(LogLevelDebug, "(%s) rdy: %d (last message received %s)",
			conn.String(), rdyCount, lastMsgDuration)
		if rdyCount > 0 {
			if lastMsgDuration > c.config.LowRdyIdleTimeout {
				c.log(LogLevelDebug, "(%s) idle connection, giving up RDY", conn.String())
				c.updateRDY(conn, 0)
			} else if lastRdyDuration > c.config.LowRdyTimeout {
				c.log(LogLevelDebug, "(%s) RDY timeout, giving up RDY", conn.String())
				c.updateRDY(conn, 0)
			}
		}
		possibleConns = append(possibleConns, conn)
	}

	availableMaxInFlight := int64(maxInFlight) - atomic.LoadInt64(&c.totalRdyCount)
	if c.inBackoff() {
		availableMaxInFlight = 1 - atomic.LoadInt64(&c.totalRdyCount)
	}

	for len(possibleConns) > 0 && availableMaxInFlight > 0 {
		availableMaxInFlight--
		c.rngMtx.Lock()
		i := c.rng.Int() % len(possibleConns)
		c.rngMtx.Unlock()
		conn := possibleConns[i]
		// delete
		possibleConns = append(possibleConns[:i], possibleConns[i+1:]...)
		c.log(LogLevelDebug, "(%s) redistributing RDY", conn.String())
		c.updateRDY(conn, 1)
	}
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (c *nsqConsumer) Stop() {
	if !atomic.CompareAndSwapInt32(&c.stopFlag, 0, 1) {
		return
	}

	c.log(LogLevelInfo, "stopping...")

	if len(c.conns()) == 0 {
		c.stopHandlers()
	} else {
		for _, conn := range c.conns() {
			err := conn.WriteCommand(StartClose())
			if err != nil {
				c.log(LogLevelError, "(%s) error sending CLS - %s", conn.String(), err)
			}
		}

		time.AfterFunc(time.Second*30, func() {
			// if we've waited this long handlers are blocked on processing messages
			// so we can't just stopHandlers (if any adtl. messages were pending processing
			// we would cause a panic on channel close)
			//
			// instead, we just bypass handler closing and skip to the final exit
			c.exit()
		})
	}
}

func (c *nsqConsumer) GetStopChan() chan int {
	return c.StopChan
}

func (c *nsqConsumer) stopHandlers() {
	c.stopHandler.Do(func() {
		c.log(LogLevelInfo, "stopping handlers")
		close(c.incomingMessages)
	})
}

// AddHandler sets the Handler for messages received by this Consumec. This can be called
// multiple times to add additional handlers. Handler will have a 1:1 ratio to message handling goroutines.
//
// This panics if called after connecting to NSQD or NSQ Lookupd
//
// (see Handler or HandlerFunc for details on implementing this interface)
func (c *nsqConsumer) AddHandler(handler Handler) {
	c.AddConcurrentHandlers(handler, 1)
}

// AddConcurrentHandlers sets the Handler for messages received by this Consumec.  It
// takes a second argument which indicates the number of goroutines to spawn for
// message handling.
//
// This panics if called after connecting to NSQD or NSQ Lookupd
//
// (see Handler or HandlerFunc for details on implementing this interface)
func (c *nsqConsumer) AddConcurrentHandlers(handler Handler, concurrency int) {
	if atomic.LoadInt32(&c.connectedFlag) == 1 {
		panic("already connected")
	}

	atomic.AddInt32(&c.runningHandlers, int32(concurrency))
	for i := 0; i < concurrency; i++ {
		go c.handlerLoop(handler)
	}
}

func (c *nsqConsumer) handlerLoop(handler Handler) {
	c.log(LogLevelDebug, "starting Handler")

	for {
		message, ok := <-c.incomingMessages
		if !ok {
			goto exit
		}

		if c.shouldFailMessage(message, handler) {
			message.Finish()
			continue
		}

		err := handler.HandleMessage(message)
		if err != nil {
			c.log(LogLevelError, "Handler returned error (%s) for msg %s", err, message.ID)
			if !message.IsAutoResponseDisabled() {
				message.Requeue(-1)
			}
			continue
		}

		if !message.IsAutoResponseDisabled() {
			message.Finish()
		}
	}

exit:
	c.log(LogLevelDebug, "stopping Handler")
	if atomic.AddInt32(&c.runningHandlers, -1) == 0 {
		c.exit()
	}
}

func (c *nsqConsumer) shouldFailMessage(message *Message, handler interface{}) bool {
	// message passed the max number of attempts
	if c.config.MaxAttempts > 0 && message.Attempts > c.config.MaxAttempts {
		c.log(LogLevelWarning, "msg %s attempted %d times, giving up",
			message.ID, message.Attempts)

		logger, ok := handler.(FailedMessageLogger)
		if ok {
			logger.LogFailedMessage(message)
		}

		return true
	}
	return false
}

func (c *nsqConsumer) exit() {
	c.exitHandler.Do(func() {
		close(c.exitChan)
		c.wg.Wait()
		close(c.StopChan)
	})
}

func (c *nsqConsumer) log(lvl LogLevel, line string, args ...interface{}) {
	c.loggerCarrier.Log(
		lvl,
		fmt.Sprintf(
			"%-4s %3d [%s/%s] %s",
			lvl,
			c.id,
			c.topic,
			c.channel,
			fmt.Sprintf(line, args...),
		),
	)
}
