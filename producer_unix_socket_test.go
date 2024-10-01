package nsq

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnixSocketProducerConnection(t *testing.T) {
	config := NewConfig()

	w, _ := NewProducer("/tmp/nsqd.sock", config)
	w.SetLogger(nullLogger, LogLevelInfo)

	err := w.Publish("write_test", []byte("test"))
	if err != nil {
		t.Fatalf("should lazily connect - %s", err)
	}

	w.Stop()

	err = w.Publish("write_test", []byte("fail test"))
	if err != ErrStopped {
		t.Fatalf("should not be able to write after Stop()")
	}
}

func TestUnixSocketProducerPing(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	config := NewConfig()
	w, _ := NewProducer("/tmp/nsqd.sock", config)
	w.SetLogger(nullLogger, LogLevelInfo)

	err := w.Ping()

	if err != nil {
		t.Fatalf("should connect on ping")
	}

	w.Stop()

	err = w.Ping()
	if err != ErrStopped {
		t.Fatalf("should not be able to ping after Stop()")
	}
}

func TestUnixSocketProducerPublish(t *testing.T) {
	topicName := "publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	config := NewConfig()
	w, _ := NewProducer("/tmp/nsqd.sock", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	for i := 0; i < msgCount; i++ {
		err := w.Publish(topicName, []byte("publish_test_case"))
		if err != nil {
			t.Fatalf("error %s", err)
		}
	}

	err := w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readMessagesFromUnixSocket(topicName, t, msgCount)
}

func TestUnixSocketProducerMultiPublish(t *testing.T) {
	topicName := "multi_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	config := NewConfig()
	w, _ := NewProducer("/tmp/nsqd.sock", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	var testData [][]byte
	for i := 0; i < msgCount; i++ {
		testData = append(testData, []byte("multipublish_test_case"))
	}

	err := w.MultiPublish(topicName, testData)
	if err != nil {
		t.Fatalf("error %s", err)
	}

	err = w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readMessagesFromUnixSocket(topicName, t, msgCount)
}

func TestUnixSocketProducerPublishAsync(t *testing.T) {
	topicName := "async_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	config := NewConfig()
	w, _ := NewProducer("/tmp/nsqd.sock", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	responseChan := make(chan *ProducerTransaction, msgCount)
	for i := 0; i < msgCount; i++ {
		err := w.PublishAsync(topicName, []byte("publish_test_case"), responseChan, "test")
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	for i := 0; i < msgCount; i++ {
		trans := <-responseChan
		if trans.Error != nil {
			t.Fatalf(trans.Error.Error())
		}
		if trans.Args[0].(string) != "test" {
			t.Fatalf(`proxied arg "%s" != "test"`, trans.Args[0].(string))
		}
	}

	err := w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readMessagesFromUnixSocket(topicName, t, msgCount)
}

func TestUnixSocketProducerMultiPublishAsync(t *testing.T) {
	topicName := "multi_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	config := NewConfig()
	w, _ := NewProducer("/tmp/nsqd.sock", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	var testData [][]byte
	for i := 0; i < msgCount; i++ {
		testData = append(testData, []byte("multipublish_test_case"))
	}

	responseChan := make(chan *ProducerTransaction)
	err := w.MultiPublishAsync(topicName, testData, responseChan, "test0", 1)
	if err != nil {
		t.Fatalf(err.Error())
	}

	trans := <-responseChan
	if trans.Error != nil {
		t.Fatalf(trans.Error.Error())
	}
	if trans.Args[0].(string) != "test0" {
		t.Fatalf(`proxied arg "%s" != "test0"`, trans.Args[0].(string))
	}
	if trans.Args[1].(int) != 1 {
		t.Fatalf(`proxied arg %d != 1`, trans.Args[1].(int))
	}

	err = w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readMessagesFromUnixSocket(topicName, t, msgCount)
}

func TestUnixSocketProducerHeartbeat(t *testing.T) {
	topicName := "heartbeat" + strconv.Itoa(int(time.Now().Unix()))

	config := NewConfig()
	config.HeartbeatInterval = 100 * time.Millisecond
	w, _ := NewProducer("/tmp/nsqd.sock", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	err := w.Publish(topicName, []byte("publish_test_case"))
	if err == nil {
		t.Fatalf("error should not be nil")
	}
	if identifyError, ok := err.(ErrIdentify); !ok ||
		identifyError.Reason != "E_BAD_BODY IDENTIFY heartbeat interval (100) is invalid" {
		t.Fatalf("wrong error - %s", err)
	}

	config = NewConfig()
	config.HeartbeatInterval = 1000 * time.Millisecond
	w, _ = NewProducer("/tmp/nsqd.sock", config)
	w.SetLogger(nullLogger, LogLevelInfo)
	defer w.Stop()

	err = w.Publish(topicName, []byte("publish_test_case"))
	if err != nil {
		t.Fatalf(err.Error())
	}

	time.Sleep(1100 * time.Millisecond)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		err := w.Publish(topicName, []byte("publish_test_case"))
		if err != nil {
			t.Fatalf("error %s", err)
		}
	}

	err = w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("error %s", err)
	}

	readMessagesFromUnixSocket(topicName, t, msgCount+1)
}

func readMessagesFromUnixSocket(topicName string, t *testing.T, msgCount int) {
	config := NewConfig()
	config.DefaultRequeueDelay = 0
	config.MaxBackoffDuration = 50 * time.Millisecond
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(nullLogger, LogLevelInfo)

	h := &ConsumerHandler{
		t: t,
		q: q,
	}
	q.AddHandler(h)

	err := q.ConnectToNSQD("/tmp/nsqd.sock")
	if err != nil {
		t.Fatalf(err.Error())
	}
	<-q.StopChan

	if h.messagesGood != msgCount {
		t.Fatalf("end of test. should have handled a diff number of messages %d != %d", h.messagesGood, msgCount)
	}

	if h.messagesFailed != 1 {
		t.Fatal("failed message not done")
	}
}

type mockProducerUnixSocketConn struct {
	delegate ConnDelegate
	closeCh  chan struct{}
	pubCh    chan struct{}
}

func newMockProducerUnixSocketConn(delegate ConnDelegate) producerConn {
	m := &mockProducerUnixSocketConn{
		delegate: delegate,
		closeCh:  make(chan struct{}),
		pubCh:    make(chan struct{}, 4),
	}
	go m.router()
	return m
}

func (m *mockProducerUnixSocketConn) String() string {
	return "/tmp/nsqd.sock"
}

func (m *mockProducerUnixSocketConn) SetLogger(logger logger, level LogLevel, prefix string) {}

func (m *mockProducerUnixSocketConn) SetLoggerLevel(lvl LogLevel) {}

func (m *mockProducerUnixSocketConn) SetLoggerForLevel(logger logger, level LogLevel, format string) {
}

func (m *mockProducerUnixSocketConn) Connect() (*IdentifyResponse, error) {
	return &IdentifyResponse{}, nil
}

func (m *mockProducerUnixSocketConn) Close() error {
	close(m.closeCh)
	return nil
}

func (m *mockProducerUnixSocketConn) WriteCommand(cmd *Command) error {
	if bytes.Equal(cmd.Name, []byte("PUB")) {
		m.pubCh <- struct{}{}
	}
	return nil
}

func (m *mockProducerUnixSocketConn) router() {
	for {
		select {
		case <-m.closeCh:
			goto exit
		case <-m.pubCh:
			m.delegate.OnResponse(nil, framedResponse(FrameTypeResponse, []byte("OK")))
		}
	}
exit:
}

func BenchmarkUnixSocketProducer(b *testing.B) {
	b.StopTimer()
	body := make([]byte, 512)

	config := NewConfig()
	p, _ := NewProducer("/tmp/nsqd.sock", config)

	p.conn = newMockProducerUnixSocketConn(&producerConnDelegate{p})
	atomic.StoreInt32(&p.state, StateConnected)
	p.closeChan = make(chan int)
	go p.router()

	startCh := make(chan struct{})
	var wg sync.WaitGroup
	parallel := runtime.GOMAXPROCS(0)

	for j := 0; j < parallel; j++ {
		wg.Add(1)
		go func() {
			<-startCh
			for i := 0; i < b.N/parallel; i++ {
				p.Publish("test", body)
			}
			wg.Done()
		}()
	}

	b.StartTimer()
	close(startCh)
	wg.Wait()
}
