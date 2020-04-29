package nsq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ConnEventsHandler describes the interface that should be implemented
// for handling events that the a Conn might fire
type ConnEventsHandler interface {
	onConnResponse(c Conn, data []byte)
	onConnError(c Conn, data []byte)
	onConnHeartbeat(c Conn)
	onConnIOError(c Conn, err error)
	onConnClose(c Conn)
}

// Publisher describes the interface that should be implemented for a message
// publisher.
type Publisher interface {
	PublishAsync(
		topic string,
		body []byte,
		doneChan chan *ProducerTransaction,
		args ...interface{},
	) error
	MultiPublishAsync(
		topic string,
		body [][]byte,
		doneChan chan *ProducerTransaction,
		args ...interface{},
	) error
	Publish(topic string, body []byte) error
	MultiPublish(topic string, body [][]byte) error
	DeferredPublish(topic string, delay time.Duration, body []byte) error
	DeferredPublishAsync(
		topic string,
		delay time.Duration,
		body []byte,
		doneChan chan *ProducerTransaction,
		args ...interface{},
	) error
}

// Producer defines the public interface a producer should implement
type Producer interface {
	ConnEventsHandler
	Publisher
	fmt.Stringer

	Ping() error
	Stop()

	router()
	getState() int32
	setConn(c Conn)
	setCloseChan(ch chan int)

	SetLogger(l logger, lvl LogLevel)
}

// nsqProducer is a high-level type to publish to NSQ.
//
// A nsqProducer instance is 1:1 with a destination `nsqd`
// and will lazily connect to that instance (and re-connect)
// when Publish commands are executed.
type nsqProducer struct {
	id     int64
	addr   string
	conn   Conn
	config Config

	loggerCarrier LoggerCarrier

	responseChan chan []byte
	errorChan    chan []byte
	closeChan    chan int

	transactionChan chan *ProducerTransaction
	transactions    []*ProducerTransaction
	state           int32

	concurrentProducers int32
	stopFlag            int32
	exitChan            chan int
	wg                  sync.WaitGroup
	guard               sync.Mutex
}

// ProducerTransaction is returned by the async publish methods
// to retrieve metadata about the command after the
// response is received.
type ProducerTransaction struct {
	cmd      *Command
	doneChan chan *ProducerTransaction
	Error    error         // the error (or nil) of the publish command
	Args     []interface{} // the slice of variadic arguments passed to PublishAsync or MultiPublishAsync
}

func (t *ProducerTransaction) finish() {
	if t.doneChan != nil {
		t.doneChan <- t
	}
}

// NewProducer returns an instance of Producer for the specified address
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewProducer the values are no longer mutable (they are copied).
func NewProducer(addr string, config *Config) (Producer, error) {
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	p := &nsqProducer{
		id: atomic.AddInt64(&instCount, 1),

		addr:   addr,
		config: *config,

		loggerCarrier: NewDefaultLoggerCarrier(),

		transactionChan: make(chan *ProducerTransaction),
		exitChan:        make(chan int),
		responseChan:    make(chan []byte),
		errorChan:       make(chan []byte),
	}

	return p, nil
}

func (p *nsqProducer) getState() int32 {
	return p.state
}

func (p *nsqProducer) setConn(conn Conn) {
	p.conn = conn
}

func (p *nsqProducer) setCloseChan(ch chan int) {
	p.closeChan = ch
}

// Ping causes the Producer to connect to it's configured nsqd (if not already
// connected) and send a `Nop` command, returning any error that might occur.
//
// This method can be used to verify that a newly-created Producer instance is
// configured correctly, rather than relying on the lazy "connect on Publish"
// behavior of a Producer.
func (p *nsqProducer) Ping() error {
	if atomic.LoadInt32(&p.state) != StateConnected {
		err := p.connect()
		if err != nil {
			return err
		}
	}

	return p.conn.WriteCommand(Nop())
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (p *nsqProducer) SetLogger(l logger, lvl LogLevel) {
	p.loggerCarrier.SetLogger(l, lvl, "")
}

// SetLoggerForLevel assigns the same logger for specified `level`.
func (p *nsqProducer) SetLoggerForLevel(l logger, lvl LogLevel) {
	p.loggerCarrier.SetLoggerForLevel(l, lvl, "(%s)")
}

// SetLoggerLevel sets the package logging level.
func (p *nsqProducer) SetLoggerLevel(lvl LogLevel) {
	p.loggerCarrier.SetLoggerLevel(lvl)
}

// String returns the address of the Producer
func (p *nsqProducer) String() string {
	return p.addr
}

// Stop initiates a graceful stop of the Producer (permanent)
//
// NOTE: this blocks until completion
func (p *nsqProducer) Stop() {
	p.guard.Lock()
	if !atomic.CompareAndSwapInt32(&p.stopFlag, 0, 1) {
		p.guard.Unlock()
		return
	}
	p.log(LogLevelInfo, "stopping")
	close(p.exitChan)
	p.close()
	p.guard.Unlock()
	p.wg.Wait()
}

// PublishAsync publishes a message body to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (p *nsqProducer) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	return p.sendCommandAsync(Publish(topic, body), doneChan, args)
}

// MultiPublishAsync publishes a slice of message bodies to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (p *nsqProducer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return p.sendCommandAsync(cmd, doneChan, args)
}

// Publish synchronously publishes a message body to the specified topic, returning
// an error if publish failed
func (p *nsqProducer) Publish(topic string, body []byte) error {
	return p.sendCommand(Publish(topic, body))
}

// MultiPublish synchronously publishes a slice of message bodies to the specified topic, returning
// an error if publish failed
func (p *nsqProducer) MultiPublish(topic string, body [][]byte) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return p.sendCommand(cmd)
}

// DeferredPublish synchronously publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires, returning
// an error if publish failed
func (p *nsqProducer) DeferredPublish(topic string, delay time.Duration, body []byte) error {
	return p.sendCommand(DeferredPublish(topic, delay, body))
}

// DeferredPublishAsync publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (p *nsqProducer) DeferredPublishAsync(topic string, delay time.Duration, body []byte,
	doneChan chan *ProducerTransaction, args ...interface{}) error {
	return p.sendCommandAsync(DeferredPublish(topic, delay, body), doneChan, args)
}

func (p *nsqProducer) sendCommand(cmd *Command) error {
	doneChan := make(chan *ProducerTransaction)
	err := p.sendCommandAsync(cmd, doneChan, nil)
	if err != nil {
		close(doneChan)
		return err
	}
	t := <-doneChan
	return t.Error
}

func (p *nsqProducer) sendCommandAsync(cmd *Command, doneChan chan *ProducerTransaction,
	args []interface{}) error {
	// keep track of how many outstanding producers we're dealing with
	// in order to later ensure that we clean them all up...
	atomic.AddInt32(&p.concurrentProducers, 1)
	defer atomic.AddInt32(&p.concurrentProducers, -1)

	if atomic.LoadInt32(&p.state) != StateConnected {
		err := p.connect()
		if err != nil {
			return err
		}
	}

	t := &ProducerTransaction{
		cmd:      cmd,
		doneChan: doneChan,
		Args:     args,
	}

	select {
	case p.transactionChan <- t:
	case <-p.exitChan:
		return ErrStopped
	}

	return nil
}

func (p *nsqProducer) connect() error {
	p.guard.Lock()
	defer p.guard.Unlock()

	if atomic.LoadInt32(&p.stopFlag) == 1 {
		return ErrStopped
	}

	switch state := atomic.LoadInt32(&p.state); state {
	case StateInit:
	case StateConnected:
		return nil
	default:
		return ErrNotConnected
	}

	p.log(LogLevelInfo, "(%s) connecting to nsqd", p.addr)

	p.conn = NewConn(p.addr, &p.config, &producerConnDelegate{p})
	p.conn.SetLoggerLevel(p.loggerCarrier.GetLogLevel())
	format := fmt.Sprintf("%3d (%%s)", p.id)
	for index, l := range p.loggerCarrier.GetLoggers() {
		p.conn.SetLoggerForLevel(l, LogLevel(index), format)
	}

	_, err := p.conn.Connect()
	if err != nil {
		p.conn.Close()
		p.log(LogLevelError, "(%s) error connecting to nsqd - %s", p.addr, err)
		return err
	}
	atomic.StoreInt32(&p.state, StateConnected)
	p.closeChan = make(chan int)
	p.wg.Add(1)
	go p.router()

	return nil
}

func (p *nsqProducer) close() {
	if !atomic.CompareAndSwapInt32(&p.state, StateConnected, StateDisconnected) {
		return
	}
	p.conn.Close()
	go func() {
		// we need to handle this in a goroutine so we don't
		// block the caller from making progress
		p.wg.Wait()
		atomic.StoreInt32(&p.state, StateInit)
	}()
}

func (p *nsqProducer) router() {
	for {
		select {
		case t := <-p.transactionChan:
			p.transactions = append(p.transactions, t)
			err := p.conn.WriteCommand(t.cmd)
			if err != nil {
				p.log(LogLevelError, "(%s) sending command - %s", p.conn.String(), err)
				p.close()
			}
		case data := <-p.responseChan:
			p.popTransaction(FrameTypeResponse, data)
		case data := <-p.errorChan:
			p.popTransaction(FrameTypeError, data)
		case <-p.closeChan:
			goto exit
		case <-p.exitChan:
			goto exit
		}
	}

exit:
	p.transactionCleanup()
	p.wg.Done()
	p.log(LogLevelInfo, "exiting router")
}

func (p *nsqProducer) popTransaction(frameType int32, data []byte) {
	t := p.transactions[0]
	p.transactions = p.transactions[1:]
	if frameType == FrameTypeError {
		t.Error = ErrProtocol{string(data)}
	}
	t.finish()
}

func (p *nsqProducer) transactionCleanup() {
	// clean up transactions we can easily account for
	for _, t := range p.transactions {
		t.Error = ErrNotConnected
		t.finish()
	}
	p.transactions = p.transactions[:0]

	// spin and free up any writes that might have raced
	// with the cleanup process (blocked on writing
	// to transactionChan)
	for {
		select {
		case t := <-p.transactionChan:
			t.Error = ErrNotConnected
			t.finish()
		default:
			// keep spinning until there are 0 concurrent producers
			if atomic.LoadInt32(&p.concurrentProducers) == 0 {
				return
			}
			// give the runtime a chance to schedule other racing goroutines
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (p *nsqProducer) log(lvl LogLevel, line string, args ...interface{}) {
	p.loggerCarrier.Log(lvl, line, p.String(), args...)
}

func (p *nsqProducer) onConnResponse(c Conn, data []byte) { p.responseChan <- data }
func (p *nsqProducer) onConnError(c Conn, data []byte)    { p.errorChan <- data }
func (p *nsqProducer) onConnHeartbeat(c Conn)             {}
func (p *nsqProducer) onConnIOError(c Conn, err error)    { p.close() }
func (p *nsqProducer) onConnClose(c Conn) {
	p.guard.Lock()
	defer p.guard.Unlock()
	close(p.closeChan)
}
