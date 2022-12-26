package nsq

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func SendMessageToUnixSocket(t *testing.T, addr string, topic string, method string, body []byte) {
	httpclient := http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", addr)
			},
		},
	}

	endpoint := fmt.Sprintf("http://unix/%s?topic=%s", method, topic)
	resp, err := httpclient.Post(endpoint, "application/octet-stream", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	if resp.StatusCode != 200 {
		t.Fatalf("%s status code: %d", method, resp.StatusCode)
	}
	resp.Body.Close()
}

func TestUnixSocketConsumer(t *testing.T) {
	consumerUnixSocketTest(t, nil)
}

func TestUnixSocketConsumerTLS(t *testing.T) {
	consumerUnixSocketTest(t, func(c *Config) {
		c.TlsV1 = true
		c.TlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	})
}

func TestUnixSocketConsumerDeflate(t *testing.T) {
	consumerUnixSocketTest(t, func(c *Config) {
		c.Deflate = true
	})
}

func TestUnixSocketConsumerSnappy(t *testing.T) {
	consumerUnixSocketTest(t, func(c *Config) {
		c.Snappy = true
	})
}

func TestUnixSocketConsumerTLSDeflate(t *testing.T) {
	consumerUnixSocketTest(t, func(c *Config) {
		c.TlsV1 = true
		c.TlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
		c.Deflate = true
	})
}

func TestUnixSocketConsumerTLSSnappy(t *testing.T) {
	consumerUnixSocketTest(t, func(c *Config) {
		c.TlsV1 = true
		c.TlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
		c.Snappy = true
	})
}

func TestUnixSocketConsumerTLSClientCert(t *testing.T) {
	cert, _ := tls.LoadX509KeyPair("./test/client.pem", "./test/client.key")
	consumerUnixSocketTest(t, func(c *Config) {
		c.TlsV1 = true
		c.TlsConfig = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true,
		}
	})
}

func TestUnixSocketConsumerTLSClientCertViaSet(t *testing.T) {
	consumerUnixSocketTest(t, func(c *Config) {
		c.Set("tls_v1", true)
		c.Set("tls_cert", "./test/client.pem")
		c.Set("tls_key", "./test/client.key")
		c.Set("tls_insecure_skip_verify", true)
	})
}

func consumerUnixSocketTest(t *testing.T, cb func(c *Config)) {
	// unix addresses of nsqd are specified in test.sh
	addr := "/tmp/nsqd.sock"
	httpAddr := "/tmp/nsqd-http.sock"

	config := NewConfig()

	config.DefaultRequeueDelay = 0
	// so that the test won't timeout from backing off
	config.MaxBackoffDuration = time.Millisecond * 50
	if cb != nil {
		cb(config)
	}
	topicName := "rdr_test"
	if config.Deflate {
		topicName = topicName + "_deflate"
	} else if config.Snappy {
		topicName = topicName + "_snappy"
	}
	if config.TlsV1 {
		topicName = topicName + "_tls"
	}
	topicName = topicName + strconv.Itoa(int(time.Now().Unix()))
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)

	h := &MyTestHandler{
		t: t,
		q: q,
	}
	q.AddHandler(h)

	SendMessageToUnixSocket(t, httpAddr, topicName, "pub", []byte(`{"msg":"single"}`))
	SendMessageToUnixSocket(t, httpAddr, topicName, "mpub", []byte("{\"msg\":\"double\"}\n{\"msg\":\"double\"}"))
	SendMessageToUnixSocket(t, httpAddr, topicName, "pub", []byte("TOBEFAILED"))
	h.messagesSent = 4

	err := q.ConnectToNSQD(addr)
	if err != nil {
		t.Fatal(err)
	}

	stats := q.Stats()
	if stats.Connections == 0 {
		t.Fatal("stats report 0 connections (should be > 0)")
	}

	err = q.ConnectToNSQD(addr)
	if err == nil {
		t.Fatal("should not be able to connect to the same NSQ twice")
	}

	conn := q.conns()[0]
	if conn.String() != addr {
		t.Fatal("connection should be bound to the specified address:", addr)
	}

	err = q.DisconnectFromNSQD("/tmp/doesntexist.sock")
	if err == nil {
		t.Fatal("should not be able to disconnect from an unknown nsqd")
	}

	err = q.ConnectToNSQD("/tmp/doesntexist.sock")
	if err == nil {
		t.Fatal("should not be able to connect to non-existent nsqd")
	}

	err = q.DisconnectFromNSQD("/tmp/doesntexist.sock")
	if err != nil {
		t.Fatal("should be able to disconnect from an nsqd - " + err.Error())
	}

	<-q.StopChan

	stats = q.Stats()
	if stats.Connections != 0 {
		t.Fatalf("stats report %d active connections (should be 0)", stats.Connections)
	}

	stats = q.Stats()
	if stats.MessagesReceived != uint64(h.messagesReceived+h.messagesFailed) {
		t.Fatalf("stats report %d messages received (should be %d)",
			stats.MessagesReceived,
			h.messagesReceived+h.messagesFailed)
	}

	if h.messagesReceived != 8 || h.messagesSent != 4 {
		t.Fatalf("end of test. should have handled a diff number of messages (got %d, sent %d)", h.messagesReceived, h.messagesSent)
	}
	if h.messagesFailed != 1 {
		t.Fatal("failed message not done")
	}
}
