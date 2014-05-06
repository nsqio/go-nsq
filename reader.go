package nsq

import (
	"bytes"
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

// HandlerFunc is a convenience type to avoid having to declare a struct
// to implement the Handler interface, it can be used like this:
//
// 	reader.SetHandler(nsq.HandlerFunc(func(m *Message) error {
// 		// handle the message
// 	})
type HandlerFunc func(message *Message) error

// HandleMessage implements the Handler interface
func (h HandlerFunc) HandleMessage(m *Message) error {
	return h(m)
}

// FailedMessageLogger is an interface that can be implemented by handlers that wish
// to receive a callback when a message is deemed "failed" (i.e. the number of attempts
// exceeded the Reader specified MaxAttemptCount)
type FailedMessageLogger interface {
	LogFailedMessage(message *Message)
}

var instCount int64

// Reader is a high-level type to consume from NSQ.
//
// A Reader instance is supplied a Handler that will be executed
// concurrently via goroutines to handle processing the stream of messages
// consumed from the specified topic/channel. See: Handler/HandlerFunc
// for details on implementing the interface to create handlers.
//
// If configured, it will poll nsqlookupd instances and handle connection (and
// reconnection) to any discovered nsqds.
type Reader struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messagesReceived uint64
	messagesFinished uint64
	messagesRequeued uint64
	totalRdyCount    int64
	backoffDuration  int64

	mtx sync.RWMutex

	logger *log.Logger
	logLvl LogLevel

	id      int64
	topic   string
	channel string
	config  *Config

	backoffChan          chan bool
	rdyChan              chan *Conn
	needRDYRedistributed int32
	backoffCounter       int32

	incomingMessages chan *Message

	rdyRetryMtx    sync.RWMutex
	rdyRetryTimers map[string]*time.Timer

	pendingConnections map[string]bool
	connections        map[string]*Conn

	// used at connection close to force a possible reconnect
	lookupdRecheckChan chan int
	lookupdHTTPAddrs   []string
	lookupdQueryIndex  int

	wg              sync.WaitGroup
	runningHandlers int32
	stopFlag        int32
	stopHandler     sync.Once

	// read from this channel to block until reader is cleanly stopped
	StopChan chan int
	exitChan chan int
}

// NewReader creates a new instance of Reader for the specified topic/channel
//
// The returned Reader instance is setup with sane default values.  To modify
// configuration, update the values on the returned instance before connecting.
func NewReader(topic string, channel string, config *Config) (*Reader, error) {
	if !IsValidTopicName(topic) {
		return nil, errors.New("invalid topic name")
	}

	if !IsValidChannelName(channel) {
		return nil, errors.New("invalid channel name")
	}

	q := &Reader{
		id: atomic.AddInt64(&instCount, 1),

		topic:   topic,
		channel: channel,
		config:  config,

		incomingMessages: make(chan *Message),

		rdyRetryTimers:     make(map[string]*time.Timer),
		pendingConnections: make(map[string]bool),
		connections:        make(map[string]*Conn),

		lookupdRecheckChan: make(chan int, 1),
		backoffChan:        make(chan bool),
		rdyChan:            make(chan *Conn, 1),

		StopChan: make(chan int),
		exitChan: make(chan int),
	}
	q.wg.Add(1)
	go q.rdyLoop()
	return q, nil
}

func (q *Reader) conns() []*Conn {
	q.mtx.RLock()
	conns := make([]*Conn, 0, len(q.connections))
	for _, c := range q.connections {
		conns = append(conns, c)
	}
	q.mtx.RUnlock()
	return conns
}

// SetLogger assigns the logger to use as well as a level
func (q *Reader) SetLogger(logger *log.Logger, lvl LogLevel) {
	q.logger = logger
	q.logLvl = lvl
}

// perConnMaxInFlight calculates the per-connection max-in-flight count.
//
// This may change dynamically based on the number of connections to nsqd the Reader
// is responsible for.
func (q *Reader) perConnMaxInFlight() int64 {
	b := float64(q.maxInFlight())
	s := b / float64(len(q.conns()))
	return int64(math.Min(math.Max(1, s), b))
}

// IsStarved indicates whether any connections for this reader are blocked on processing
// before being able to receive more messages (ie. RDY count of 0 and not exiting)
func (q *Reader) IsStarved() bool {
	for _, conn := range q.conns() {
		threshold := int64(float64(atomic.LoadInt64(&conn.lastRdyCount)) * 0.85)
		inFlight := atomic.LoadInt64(&conn.messagesInFlight)
		if inFlight >= threshold && inFlight > 0 && !conn.IsClosing() {
			return true
		}
	}
	return false
}

func (q *Reader) maxInFlight() int {
	q.config.RLock()
	mif := q.config.maxInFlight
	q.config.RUnlock()
	return mif
}

// ConnectToNSQLookupd adds an nsqlookupd address to the list for this Reader instance.
//
// If it is the first to be added, it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (q *Reader) ConnectToNSQLookupd(addr string) error {
	q.mtx.Lock()
	for _, x := range q.lookupdHTTPAddrs {
		if x == addr {
			q.mtx.Unlock()
			return ErrLookupdAddressExists
		}
	}
	q.lookupdHTTPAddrs = append(q.lookupdHTTPAddrs, addr)
	numLookupd := len(q.lookupdHTTPAddrs)
	q.mtx.Unlock()

	// if this is the first one, kick off the go loop
	if numLookupd == 1 {
		q.queryLookupd()
		q.wg.Add(1)
		go q.lookupdLoop()
	}

	return nil
}

// poll all known lookup servers every LookupdPollInterval
func (q *Reader) lookupdLoop() {
	// add some jitter so that multiple consumers discovering the same topic,
	// when restarted at the same time, dont all connect at once.
	rand.Seed(time.Now().UnixNano())

	jitter := time.Duration(int64(rand.Float64() *
		q.config.lookupdPollJitter * float64(q.config.lookupdPollInterval)))
	ticker := time.NewTicker(q.config.lookupdPollInterval)

	select {
	case <-time.After(jitter):
	case <-q.exitChan:
		goto exit
	}

	for {
		select {
		case <-ticker.C:
			q.queryLookupd()
		case <-q.lookupdRecheckChan:
			q.queryLookupd()
		case <-q.exitChan:
			goto exit
		}
	}

exit:
	ticker.Stop()
	q.log(LogLevelInfo, "exiting lookupdLoop")
	q.wg.Done()
}

// make an HTTP req to the /lookup endpoint of one of the
// configured nsqlookupd instances to discover which nsqd provide
// the topic we are consuming.
//
// initiate a connection to any new producers that are identified.
func (q *Reader) queryLookupd() {
	q.mtx.RLock()
	addr := q.lookupdHTTPAddrs[q.lookupdQueryIndex]
	num := len(q.lookupdHTTPAddrs)
	q.mtx.RUnlock()
	q.lookupdQueryIndex = (q.lookupdQueryIndex + 1) % num
	endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(q.topic))

	q.log(LogLevelInfo, "querying nsqlookupd %s", endpoint)

	data, err := apiRequest(endpoint)
	if err != nil {
		q.log(LogLevelError, "error querying nsqlookupd (%s) - %s", addr, err)
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
	for i := range data.Get("producers").MustArray() {
		producer := data.Get("producers").GetIndex(i)
		address := producer.Get("address").MustString()
		broadcastAddress, ok := producer.CheckGet("broadcast_address")
		if ok {
			address = broadcastAddress.MustString()
		}
		port := producer.Get("tcp_port").MustInt()

		// make an address, start a connection
		joined := net.JoinHostPort(address, strconv.Itoa(port))
		err = q.ConnectToNSQD(joined)
		if err != nil && err != ErrAlreadyConnected {
			q.log(LogLevelError, "(%s) error connecting to nsqd - %s", joined, err)
			continue
		}
	}
}

// ConnectToNSQD takes a nsqd address to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to a single, local,
// instance.
func (q *Reader) ConnectToNSQD(addr string) error {
	if atomic.LoadInt32(&q.stopFlag) == 1 {
		return errors.New("reader stopped")
	}

	if atomic.LoadInt32(&q.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	_, pendingOk := q.pendingConnections[addr]
	q.mtx.RLock()
	_, ok := q.connections[addr]
	q.mtx.RUnlock()

	if ok || pendingOk {
		return ErrAlreadyConnected
	}

	q.log(LogLevelInfo, "(%s) connecting to nsqd", addr)

	conn := NewConn(addr, q.config)
	conn.SetLogger(q.logger, q.logLvl,
		fmt.Sprintf("%3d [%s/%s] (%%s)", q.id, q.topic, q.channel))
	conn.Delegate = &readerConnDelegate{q}

	cleanupConnection := func() {
		q.mtx.Lock()
		delete(q.pendingConnections, addr)
		q.mtx.Unlock()
		conn.Close()
	}

	q.pendingConnections[addr] = true

	resp, err := conn.Connect()
	if err != nil {
		cleanupConnection()
		return err
	}

	if resp != nil {
		if resp.MaxRdyCount < int64(q.maxInFlight()) {
			q.log(LogLevelWarning,
				"(%s) max RDY count %d < reader max in flight %d, truncation possible",
				conn.String(), resp.MaxRdyCount, q.maxInFlight())
		}
	}

	cmd := Subscribe(q.topic, q.channel)
	err = conn.WriteCommand(cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to subscribe to %s:%s - %s",
			conn, q.topic, q.channel, err.Error())
	}

	delete(q.pendingConnections, addr)
	q.mtx.Lock()
	q.connections[addr] = conn
	q.mtx.Unlock()

	// pre-emptive signal to existing connections to lower their RDY count
	for _, c := range q.conns() {
		q.rdyChan <- c
	}

	return nil
}

func (q *Reader) onConnMessage(c *Conn, msg *Message) {
	atomic.AddInt64(&q.totalRdyCount, -1)
	atomic.AddUint64(&q.messagesReceived, 1)
	q.incomingMessages <- msg
	q.rdyChan <- c
}

func (q *Reader) onConnMessageFinished(c *Conn, msg *Message) {
	atomic.AddUint64(&q.messagesFinished, 1)
}

func (q *Reader) onConnMessageRequeued(c *Conn, msg *Message) {
	atomic.AddUint64(&q.messagesRequeued, 1)
}

func (q *Reader) onConnBackoff(c *Conn) {
	q.backoffChan <- false
}

func (q *Reader) onConnResume(c *Conn) {
	q.backoffChan <- true
}

func (q *Reader) onConnResponse(c *Conn, data []byte) {
	switch {
	case bytes.Equal(data, []byte("CLOSE_WAIT")):
		// server is ready for us to close (it ack'd our StartClose)
		// we can assume we will not receive any more messages over this channel
		// (but we can still write back responses)
		q.log(LogLevelInfo, "(%s) received CLOSE_WAIT from nsqd", c.String())
		c.Close()
	}
}

func (q *Reader) onConnError(c *Conn, data []byte) {
}

func (q *Reader) onConnHeartbeat(c *Conn) {
}

func (q *Reader) onConnIOError(c *Conn, err error) {
	c.Close()
}

func (q *Reader) onConnClose(c *Conn) {
	var hasRDYRetryTimer bool

	// remove this connections RDY count from the reader's total
	rdyCount := c.RDY()
	atomic.AddInt64(&q.totalRdyCount, -rdyCount)

	q.rdyRetryMtx.Lock()
	if timer, ok := q.rdyRetryTimers[c.String()]; ok {
		// stop any pending retry of an old RDY update
		timer.Stop()
		delete(q.rdyRetryTimers, c.String())
		hasRDYRetryTimer = true
	}
	q.rdyRetryMtx.Unlock()

	q.mtx.Lock()
	delete(q.connections, c.String())
	left := len(q.connections)
	q.mtx.Unlock()

	q.log(LogLevelWarning, "there are %d connections left alive", left)

	if (hasRDYRetryTimer || rdyCount > 0) &&
		(left == q.maxInFlight() || q.inBackoff()) {
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

	q.mtx.RLock()
	numLookupd := len(q.lookupdHTTPAddrs)
	q.mtx.RUnlock()
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
				q.log(LogLevelInfo, "(%s) re-connecting in 15 seconds...", addr)
				time.Sleep(15 * time.Second)
				if atomic.LoadInt32(&q.stopFlag) == 1 {
					break
				}
				err := q.ConnectToNSQD(addr)
				if err != nil && err != ErrAlreadyConnected {
					q.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
					continue
				}
				break
			}
		}(c.RemoteAddr().String())
	}
}

func (q *Reader) backoffDurationForCount(count int32) time.Duration {
	backoffDuration := q.config.backoffMultiplier *
		time.Duration(math.Pow(2, float64(count)))
	if backoffDuration > q.config.maxBackoffDuration {
		backoffDuration = q.config.maxBackoffDuration
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

			q.log(LogLevelWarning,
				"(%s) backoff timeout expired, sending RDY 1",
				choice.String())
			// while in backoff only ever let 1 message at a time through
			q.updateRDY(choice, 1)
		case c := <-q.rdyChan:
			if backoffTimer != nil || backoffCounter > 0 {
				continue
			}

			// send ready immediately
			remain := c.RDY()
			lastRdyCount := c.LastRDY()
			count := q.perConnMaxInFlight()
			// refill when at 1, or at 25%, or if connections have changed and we have too many RDY
			if remain <= 1 || remain < (lastRdyCount/4) || (count > 0 && count < remain) {
				q.log(LogLevelDebug, "(%s) sending RDY %d (%d remain from last RDY %d)",
					c.String(), count, remain, lastRdyCount)
				q.updateRDY(c, count)
			} else {
				q.log(LogLevelDebug, "(%s) skip sending RDY %d (%d remain out of last RDY %d)",
					c.String(), count, remain, lastRdyCount)
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
				maxBackoffCount := int32(math.Max(1, math.Ceil(
					math.Log2(q.config.maxBackoffDuration.Seconds()))))
				if backoffCounter < maxBackoffCount {
					backoffCounter++
					backoffUpdated = true
				}
			}

			if backoffUpdated {
				atomic.StoreInt32(&q.backoffCounter, backoffCounter)
			}

			// exit backoff
			if backoffCounter == 0 && backoffUpdated {
				count := q.perConnMaxInFlight()
				q.log(LogLevelWarning, "exiting backoff, returning all to RDY %d", count)
				for _, c := range q.conns() {
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

				q.log(LogLevelWarning, "backing off for %.04f seconds (backoff level %d), setting all to RDY 0",
					backoffDuration.Seconds(), backoffCounter)

				// send RDY 0 immediately (to *all* connections)
				for _, c := range q.conns() {
					q.updateRDY(c, 0)
				}
			}
		case <-redistributeTicker.C:
			q.redistributeRDY()
		case <-q.exitChan:
			goto exit
		}
	}

exit:
	redistributeTicker.Stop()
	if backoffTimer != nil {
		backoffTimer.Stop()
	}
	q.log(LogLevelInfo, "rdyLoop exiting")
	q.wg.Done()
}

func (q *Reader) updateRDY(c *Conn, count int64) error {
	if c.IsClosing() {
		return nil
	}

	// never exceed the nsqd's configured max RDY count
	if count > c.MaxRDY() {
		count = c.MaxRDY()
	}

	// stop any pending retry of an old RDY update
	q.rdyRetryMtx.Lock()
	if timer, ok := q.rdyRetryTimers[c.String()]; ok {
		timer.Stop()
		delete(q.rdyRetryTimers, c.String())
	}
	q.rdyRetryMtx.Unlock()

	// never exceed our global max in flight. truncate if possible.
	// this could help a new connection get partial max-in-flight
	rdyCount := c.RDY()
	maxPossibleRdy := int64(q.maxInFlight()) - atomic.LoadInt64(&q.totalRdyCount) + rdyCount
	if maxPossibleRdy > 0 && maxPossibleRdy < count {
		count = maxPossibleRdy
	}
	if maxPossibleRdy <= 0 && count > 0 {
		if rdyCount == 0 {
			// we wanted to exit a zero RDY count but we couldn't send it...
			// in order to prevent eternal starvation we reschedule this attempt
			// (if any other RDY update succeeds this timer will be stopped)
			q.rdyRetryMtx.Lock()
			q.rdyRetryTimers[c.String()] = time.AfterFunc(5*time.Second,
				func() {
					q.updateRDY(c, count)
				})
			q.rdyRetryMtx.Unlock()
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
	err := c.WriteCommand(Ready(int(count)))
	if err != nil {
		q.log(LogLevelError, "(%s) error sending RDY %d - %s", c.String(), count, err)
		return err
	}
	return nil
}

func (q *Reader) redistributeRDY() {
	if q.inBackoffBlock() {
		return
	}

	numConns := len(q.conns())
	maxInFlight := q.maxInFlight()
	if numConns > maxInFlight {
		q.log(LogLevelDebug, "redistributing RDY state (%d conns > %d max_in_flight)",
			numConns, maxInFlight)
		atomic.StoreInt32(&q.needRDYRedistributed, 1)
	}

	if q.inBackoff() && numConns > 1 {
		q.log(LogLevelDebug, "redistributing RDY state (in backoff and %d conns > 1)", numConns)
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
		q.log(LogLevelDebug, "(%s) rdy: %d (last message received %s)",
			c.String(), rdyCount, lastMsgDuration)
		if rdyCount > 0 && lastMsgDuration > q.config.lowRdyIdleTimeout {
			q.log(LogLevelDebug, "(%s) idle connection, giving up RDY", c.String())
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
		q.log(LogLevelDebug, "(%s) redistributing RDY", c.String())
		q.updateRDY(c, 1)
	}
}

// Stop will initiate a graceful stop of the Reader (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (q *Reader) Stop() {
	if !atomic.CompareAndSwapInt32(&q.stopFlag, 0, 1) {
		return
	}

	q.log(LogLevelInfo, "stopping...")

	if len(q.conns()) == 0 {
		q.stopHandlers()
	} else {
		for _, c := range q.conns() {
			err := c.WriteCommand(StartClose())
			if err != nil {
				q.log(LogLevelError, "(%s) error sending CLS - %s", c.String(), err)
			}
		}

		time.AfterFunc(time.Second*30, func() {
			q.stopHandlers()
		})
	}
}

func (q *Reader) stopHandlers() {
	q.stopHandler.Do(func() {
		q.log(LogLevelInfo, "stopping handlers")
		close(q.incomingMessages)
	})
}

// SetHandler sets the Handler for messages received by this Reader.
//
// (see Handler or HandlerFunc for details on implementing this interface)
func (q *Reader) SetHandler(handler Handler) {
	q.setHandlers(handler, 1)
}

// SetConcurrentHandlers sets the Handler for messages received by this Reader.  It
// takes a second argument which indicates the number of goroutines to spawn for
// message handling.
//
// (see Handler or HandlerFunc for details on implementing this interface)
func (q *Reader) SetConcurrentHandlers(handler Handler, concurrency int) {
	q.setHandlers(handler, concurrency)
}

func (q *Reader) setHandlers(handler Handler, concurrency int) {
	if atomic.LoadInt32(&q.runningHandlers) > 0 {
		panic("cannot call setHandlers() multiple times")
	}
	atomic.AddInt32(&q.runningHandlers, int32(concurrency))
	for i := 0; i < concurrency; i++ {
		go q.handlerLoop(handler)
	}
}

func (q *Reader) handlerLoop(handler Handler) {
	q.log(LogLevelInfo, "starting Handler")

	for {
		message, ok := <-q.incomingMessages
		if !ok {
			goto exit
		}

		if q.shouldFailMessage(message, handler) {
			message.Finish()
			continue
		}

		err := handler.HandleMessage(message)
		if err != nil {
			q.log(LogLevelError, "Handler returned error (%s) for msg %s", err, message.ID)
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
	q.log(LogLevelInfo, "stopping Handler")
	if atomic.AddInt32(&q.runningHandlers, -1) == 0 {
		q.exit()
	}
}

func (q *Reader) shouldFailMessage(message *Message, handler interface{}) bool {
	// message passed the max number of attempts
	if q.config.maxAttempts > 0 && message.Attempts > q.config.maxAttempts {
		q.log(LogLevelWarning, "msg %s attempted %d times, giving up",
			message.ID, message.Attempts)

		logger, ok := handler.(FailedMessageLogger)
		if ok {
			logger.LogFailedMessage(message)
		}

		return true
	}
	return false
}

func (q *Reader) exit() {
	close(q.exitChan)
	q.wg.Wait()
	close(q.StopChan)
}

func (q *Reader) log(lvl LogLevel, line string, args ...interface{}) {
	var prefix string

	if q.logger == nil {
		return
	}

	if q.logLvl > lvl {
		return
	}

	switch lvl {
	case LogLevelDebug:
		prefix = "DBG"
	case LogLevelInfo:
		prefix = "INF"
	case LogLevelWarning:
		prefix = "WRN"
	case LogLevelError:
		prefix = "ERR"
	}

	q.logger.Printf("%-4s %3d [%s/%s] %s",
		prefix, q.id, q.topic, q.channel,
		fmt.Sprintf(line, args...))
}
