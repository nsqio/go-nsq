package nsq

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

type ConsumerHandler struct {
	t              *testing.T
	q              *Consumer
	messagesGood   int
	messagesFailed int
}

func (h *ConsumerHandler) LogFailedMessage(message *Message) {
	h.messagesFailed++
	h.q.Stop()
}

func (h *ConsumerHandler) HandleMessage(message *Message) error {
	msg := string(message.Body)
	if msg == "bad_test_case" {
		return errors.New("fail this message")
	}
	if msg != "multipublish_test_case" && msg != "publish_test_case" {
		h.t.Error("message 'action' was not correct:", msg)
	}
	h.messagesGood++
	return nil
}

func TestProducerConnection(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	config := NewConfig()
	w, _ := NewProducer("127.0.0.1:4150", config)
	w.SetLogger(nullLogger, LogLevelInfo)

	err := w.Publish("write_test", []byte("test"))
	if err != nil {
		t.Fatalf("should lazily connect")
	}

	w.Stop()

	err = w.Publish("write_test", []byte("fail test"))
	if err != ErrStopped {
		t.Fatalf("should not be able to write after Stop()")
	}
}

func TestProducerPing(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	config := NewConfig()
	w, _ := NewProducer("127.0.0.1:4150", config)
	w.SetLogger(nullLogger, LogLevelInfo)

	err := w.Ping()
	if err != nil {
		t.Fatalf("should connect on ping")
	}

	w.Stop()

	err = w.Ping("write_test", []byte("fail test"))
	if err != ErrStopped {
		t.Fatalf("should not be able to ping after Stop()")
	}
}

func TestProducerPublish(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	config := NewConfig()
	w, _ := NewProducer("127.0.0.1:4150", config)
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

	readMessages(topicName, t, msgCount)
}

func TestProducerMultiPublish(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "multi_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	config := NewConfig()
	w, _ := NewProducer("127.0.0.1:4150", config)
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

	readMessages(topicName, t, msgCount)
}

func TestProducerPublishAsync(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "async_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	config := NewConfig()
	w, _ := NewProducer("127.0.0.1:4150", config)
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

	readMessages(topicName, t, msgCount)
}

func TestProducerMultiPublishAsync(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "multi_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	config := NewConfig()
	w, _ := NewProducer("127.0.0.1:4150", config)
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

	readMessages(topicName, t, msgCount)
}

func TestProducerHeartbeat(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "heartbeat" + strconv.Itoa(int(time.Now().Unix()))

	config := NewConfig()
	config.HeartbeatInterval = 100 * time.Millisecond
	w, _ := NewProducer("127.0.0.1:4150", config)
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
	w, _ = NewProducer("127.0.0.1:4150", config)
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

	readMessages(topicName, t, msgCount+1)
}

func readMessages(topicName string, t *testing.T, msgCount int) {
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

	err := q.ConnectToNSQD("127.0.0.1:4150")
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
