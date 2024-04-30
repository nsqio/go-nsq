package nsq

import (
	"bytes"
	goflate "compress/flate"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gosnappy "github.com/golang/snappy"
	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/snappy"
)

var ipsum = `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce ut aliquet ante. Donec consequat mollis nulla, dictum pulvinar lacus fringilla non. Curabitur euismod sagittis elit a efficitur. Maecenas laoreet lobortis metus ac viverra. Duis nec mattis quam, nec maximus odio. Aenean euismod, ante et lacinia tristique, neque nibh imperdiet quam, in aliquet nibh nibh in dui. Quisque vestibulum dui sit amet dui pellentesque interdum. Ut aliquam odio nec tortor euismod, ac iaculis urna ultrices. Ut quam libero, tristique nec sollicitudin et, suscipit id ex. Cras egestas quam eget egestas lobortis. Donec ultrices consectetur turpis, vel pharetra odio dictum sit amet. Integer pulvinar ullamcorper urna, a pellentesque ipsum rutrum sed. Aenean ac molestie lorem, pulvinar facilisis metus. In hac habitasse platea dictumst. Quisque sed leo tincidunt ligula accumsan ultrices eu in ex. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae;

Sed sollicitudin est eget lobortis aliquam. Integer tellus leo, porta vitae tempus id, sodales ac dolor. Mauris cursus pharetra lorem a venenatis. Ut facilisis eu purus vel semper. Sed viverra, ipsum sit amet auctor pulvinar, risus nibh consequat velit, euismod egestas metus nisl ac mauris. Aliquam quis auctor dolor. Mauris fermentum urna at erat fringilla, at gravida ex luctus. Phasellus quis lacus volutpat massa vestibulum ornare. Nullam maximus ullamcorper ipsum non dignissim. Nulla id mi sed nulla gravida sagittis. Nam ullamcorper massa at consequat porta. Integer in est ut nulla tincidunt finibus. Ut id urna euismod, accumsan velit at, feugiat odio.

Fusce vehicula nisl augue, ut ultrices enim scelerisque in. Integer vitae erat in lacus accumsan molestie in ac leo. Donec eleifend auctor quam eget scelerisque. Curabitur posuere erat sed erat eleifend scelerisque. Duis tortor diam, lacinia quis hendrerit vitae, varius et sapien. Cras at velit tempor, dapibus odio sed, viverra neque. Integer in ullamcorper ante. Curabitur et dictum dolor, ut faucibus risus. Nulla vel libero nulla.

Donec odio erat, vulputate in vulputate tempus, rutrum at ipsum. Phasellus diam eros, accumsan posuere orci ac, pellentesque ornare erat. Donec vestibulum ante maximus mollis vestibulum. Phasellus aliquet semper ligula, ut rhoncus enim luctus eu. Sed quis viverra ex. Donec pretium mollis justo, nec blandit risus dictum commodo. Aliquam nunc metus, facilisis nec fermentum sit amet, ultrices non ante. Cras cursus lacus nec dolor iaculis ornare. Pellentesque nibh lectus, convallis a nisi non, sollicitudin scelerisque tellus. Curabitur elit nulla, sollicitudin at nibh eu, rhoncus tincidunt dui. Aliquam sed interdum risus. Curabitur a interdum augue. Donec finibus enim velit, nec venenatis ante suscipit quis. In id nunc lacus.

Fusce et mauris et elit aliquam semper accumsan non magna. Fusce nisi ante, pretium quis mollis eu, vehicula non nisi. Proin euismod tincidunt turpis vel vehicula. Fusce ut dui sodales, ullamcorper orci sit amet, euismod nisl. Proin egestas lacus ut mollis cursus. Suspendisse libero ante, tristique vitae mi sit amet, efficitur rhoncus magna. Sed porttitor, velit vitae efficitur finibus, felis leo ultricies elit, eu suscipit purus elit ut elit.

In sit amet fermentum neque, sit amet fermentum ipsum. Curabitur mollis imperdiet lorem, in auctor ligula consequat finibus. Proin eu mi at mauris pretium suscipit. Fusce leo dolor, feugiat in lacus mattis, iaculis venenatis purus. Vivamus dictum elementum dignissim. Maecenas accumsan blandit eros, at dapibus velit ultricies vitae. Phasellus ut velit vel sapien fermentum volutpat id ac orci. Maecenas lorem diam, lacinia vitae mi et, fringilla lacinia dolor. Nullam purus elit, tempus eu gravida sit amet, scelerisque sit amet quam. Aenean non elit vitae lacus fringilla varius. Sed pharetra nisi sit amet bibendum malesuada. Fusce euismod nulla ac lectus rutrum, ac vehicula nibh maximus. Praesent eu tincidunt orci, nec varius libero. Nam vitae lorem rhoncus, hendrerit sapien placerat, varius turpis. Fusce massa ligula, venenatis nec diam ac, vestibulum commodo libero. Sed id risus ac libero posuere posuere.

Duis tincidunt condimentum tristique. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Pellentesque elementum risus et viverra faucibus. Morbi et ex vitae ante aliquam auctor ut in felis. Integer sed vehicula eros. Vivamus mattis aliquet felis condimentum ullamcorper. Nullam lacus ipsum, gravida at gravida nec, egestas ac nulla. Vestibulum risus nisi, ullamcorper venenatis tortor sed, ullamcorper fermentum eros. Morbi ultrices, mi sed euismod venenatis, leo nulla ultrices justo, nec placerat enim nunc a elit. Quisque dictum magna quis aliquam vestibulum. Duis hendrerit ante in eros lacinia, at gravida orci congue. Donec nec nibh velit.

Vivamus aliquam tellus non ipsum vestibulum, nec sollicitudin leo convallis. Nullam risus nulla, bibendum non lacus ut, bibendum pretium felis. Sed sed sed.`

func BenchmarkCompression(b *testing.B) {
	type myOps interface {
		Flush() error
		Reset(writer io.Writer)
	}

	type readResetter interface {
		Reset(reader io.Reader)
	}

	type oddResetter interface {
		Reset(r io.Reader, dict []byte) error
	}

	for _, payloadType := range []string{"compressible", "random"} {
		b.Run(payloadType, func(b *testing.B) {
			for _, size := range []int{100, 4096, 65536, 1048576, 2097152, 5048576} {
				b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
					toSend := make([]byte, size)
					if payloadType == "compressible" {
						for i := 0; i < len(toSend); {
							i += copy(toSend[i:], ipsum)
						}
					} else {
						if _, err := rand.Read(toSend); err != nil {
							b.Fatal(err)
						}
					}
					buf := bytes.NewBuffer(make([]byte, 0, size+1000))

					for _, option := range []string{"normal", "deflate1", "godeflate3", "deflate3", "deflate", "deflate9", "snappy", "gosnappy"} {
						b.Run(option, func(b *testing.B) {
							var w io.Writer
							var r io.Reader

							switch option {
							case "deflate1":
								fw, err := flate.NewWriter(buf, 1)
								if err != nil {
									b.Fatal(err)
								}
								fr := flate.NewReader(buf)
								w, r = fw, fr
								defer fw.Close()
								defer fr.Close()
							case "deflate":
								fw, err := flate.NewWriter(buf, 6)
								if err != nil {
									b.Fatal(err)
								}
								fr := flate.NewReader(buf)
								w, r = fw, fr
								defer fw.Close()
								defer fr.Close()
							case "deflate9":
								fw, err := flate.NewWriter(buf, 9)
								if err != nil {
									b.Fatal(err)
								}
								fr := flate.NewReader(buf)
								w, r = fw, fr
								defer fw.Close()
								defer fr.Close()
							case "deflate3":
								fw, err := flate.NewWriter(buf, 3)
								if err != nil {
									b.Fatal(err)
								}
								fr := flate.NewReader(buf)
								w, r = fw, fr
								defer fw.Close()
								defer fr.Close()
							case "godeflate3":
								fw, err := goflate.NewWriter(buf, 3)
								if err != nil {
									b.Fatal(err)
								}
								fr := goflate.NewReader(buf)
								w, r = fw, fr
								defer fw.Close()
								defer fr.Close()
							case "snappy":
								sw := snappy.NewBufferedWriter(buf)
								sr := snappy.NewReader(buf)
								w, r = sw, sr
								defer sw.Close()
							case "gosnappy":
								sw := gosnappy.NewBufferedWriter(buf)
								sr := gosnappy.NewReader(buf)
								w, r = sw, sr
								defer sw.Close()

							case "normal":
								w = buf
								r = buf
							}

							in := make([]byte, len(toSend))
							ops, _ := w.(myOps)
							rOps, _ := r.(readResetter)
							roOps, _ := r.(oddResetter)

							b.ResetTimer()
							b.ReportAllocs()

							for i := 0; i < b.N; i++ {
								buf.Reset()
								if ops != nil {
									ops.Reset(buf)
								}
								if rOps != nil {
									rOps.Reset(buf)
								}
								if roOps != nil {
									roOps.Reset(buf, nil)
								}

								n, err := w.Write(toSend)
								if err != nil {
									b.Fatal(err)
								}
								if n != len(toSend) {
									b.Fatalf("expected %d got %d", len(toSend), n)
								}

								if ops != nil {
									if err := ops.Flush(); err != nil {
										b.Fatal(err)
									}
								}

								b.ReportMetric(float64(buf.Len()), "cmpSizeB")

								n, err = io.ReadFull(r, in)
								if err != nil {
									b.Fatal(err)
								}
								if n != len(toSend) {
									b.Fatalf("expected %d got %d", len(toSend), n)
								}
								if !bytes.Equal(in, toSend) {
									b.Fatal("Result does not match!")
								}
							}
						})
					}
				})
			}
		})
	}
}

type benchConsumer struct {
	count  int32
	target int32
	wg     sync.WaitGroup
}

func (h *benchConsumer) HandleMessage(message *Message) error {
	if atomic.AddInt32(&h.count, 1) == h.target {
		h.wg.Done()
	}
	return nil
}

func BenchmarkSendReceive(b *testing.B) {
	tests := []struct {
		size   int
		option string
	}{
		{size: 100, option: "normal"},
		{size: 100, option: "deflate"},
		{size: 100, option: "snappy"},
		{size: 4096, option: "normal"},
		{size: 4096, option: "deflate"},
		{size: 4096, option: "snappy"},
		{size: 65536, option: "normal"},
		{size: 65536, option: "deflate"},
		{size: 65536, option: "snappy"},
		{size: 1048576, option: "normal"},
		{size: 1048576, option: "deflate"},
		{size: 1048576, option: "snappy"},
		{size: 2097152, option: "normal"},
		{size: 2097152, option: "deflate"},
		{size: 2097152, option: "snappy"},
		{size: 5048576, option: "normal"},
		{size: 5048576, option: "deflate"},
		{size: 5048576, option: "snappy"},
	}

	for _, test := range tests {
		b.Run(fmt.Sprintf("size=%d/%s", test.size, test.option), func(b *testing.B) {
			config := NewConfig()

			switch test.option {
			case "deflate":
				config.Deflate = true
			case "snappy":
				config.Snappy = true
			case "normal":
			}

			topicName := "bench_send_receive" + strconv.Itoa(int(time.Now().Unix()))
			q, err := NewConsumer(topicName, "ch", config)
			if err != nil {
				b.Fatal(err)
			}
			defer q.Stop()
			q.SetLogger(nullLogger, LogLevelInfo)

			h := &benchConsumer{
				target: int32(b.N),
			}
			h.wg.Add(1)
			q.AddHandler(h)

			if err := q.ConnectToNSQD("127.0.0.1:4150"); err != nil {
				b.Fatal(err)
			}

			p, err := NewProducer("127.0.0.1:4150", config)
			if err != nil {
				b.Fatal(err)
			}
			defer p.Stop()
			p.SetLogger(nullLogger, LogLevelInfo)

			toSend := make([]byte, test.size)
			if _, err := rand.Read(toSend); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := p.Publish(topicName, toSend); err != nil {
					b.Fatal(err)
				}
			}

			h.wg.Wait()
		})
	}
}

type MyTestHandler struct {
	t                *testing.T
	q                *Consumer
	messagesSent     int
	messagesReceived int
	messagesFailed   int
}

var nullLogger = log.New(ioutil.Discard, "", log.LstdFlags)

func (h *MyTestHandler) LogFailedMessage(message *Message) {
	h.messagesFailed++
	h.q.Stop()
}

func (h *MyTestHandler) HandleMessage(message *Message) error {
	if string(message.Body) == "TOBEFAILED" {
		h.messagesReceived++
		return errors.New("fail this message")
	}

	data := struct {
		Msg string
	}{}

	err := json.Unmarshal(message.Body, &data)
	if err != nil {
		return err
	}

	msg := data.Msg
	if msg != "single" && msg != "double" {
		h.t.Error("message 'action' was not correct: ", msg, data)
	}
	h.messagesReceived++
	return nil
}

func SendMessage(t *testing.T, port int, topic string, method string, body []byte) {
	httpclient := &http.Client{}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/%s?topic=%s", port, method, topic)
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(body))
	resp, err := httpclient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	if resp.StatusCode != 200 {
		t.Fatalf("%s status code: %d", method, resp.StatusCode)
	}
	resp.Body.Close()
}

func TestConsumer(t *testing.T) {
	consumerTest(t, nil)
}

func TestConsumerTLS(t *testing.T) {
	consumerTest(t, func(c *Config) {
		c.TlsV1 = true
		c.TlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	})
}

func TestConsumerDeflate(t *testing.T) {
	consumerTest(t, func(c *Config) {
		c.Deflate = true
	})
}

func TestConsumerSnappy(t *testing.T) {
	consumerTest(t, func(c *Config) {
		c.Snappy = true
	})
}

func TestConsumerTLSDeflate(t *testing.T) {
	consumerTest(t, func(c *Config) {
		c.TlsV1 = true
		c.TlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
		c.Deflate = true
	})
}

func TestConsumerTLSSnappy(t *testing.T) {
	consumerTest(t, func(c *Config) {
		c.TlsV1 = true
		c.TlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
		c.Snappy = true
	})
}

func TestConsumerTLSClientCert(t *testing.T) {
	cert, _ := tls.LoadX509KeyPair("./test/client.pem", "./test/client.key")
	consumerTest(t, func(c *Config) {
		c.TlsV1 = true
		c.TlsConfig = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true,
		}
	})
}

func TestConsumerLookupdAuthorization(t *testing.T) {
	// confirm that LookupAuthorization = true sets Authorization header on lookudp call
	config := NewConfig()
	config.AuthSecret = "AuthSecret"
	topicName := "auth" + strconv.Itoa(int(time.Now().Unix()))
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)

	var req bool
	lookupd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req = true
		if h := r.Header.Get("Authorization"); h != "Bearer AuthSecret" {
			t.Errorf("got Auth header %q", h)
		}
		w.WriteHeader(404)
	}))
	defer lookupd.Close()

	h := &MyTestHandler{
		t: t,
		q: q,
	}
	q.AddHandler(h)

	q.ConnectToNSQLookupd(lookupd.URL)
	if req == false {
		t.Errorf("lookupd call not completed")
	}
}

func TestConsumerTLSClientCertViaSet(t *testing.T) {
	consumerTest(t, func(c *Config) {
		c.Set("tls_v1", true)
		c.Set("tls_cert", "./test/client.pem")
		c.Set("tls_key", "./test/client.key")
		c.Set("tls_insecure_skip_verify", true)
	})
}

func consumerTest(t *testing.T, cb func(c *Config)) {
	config := NewConfig()
	laddr := "127.0.0.1"
	// so that the test can simulate binding consumer to specified address
	config.LocalAddr, _ = net.ResolveTCPAddr("tcp", laddr+":0")
	// so that the test can simulate reaching max requeues and a call to LogFailedMessage
	config.DefaultRequeueDelay = 0
	// so that the test wont timeout from backing off
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

	SendMessage(t, 4151, topicName, "pub", []byte(`{"msg":"single"}`))
	SendMessage(t, 4151, topicName, "mpub", []byte("{\"msg\":\"double\"}\n{\"msg\":\"double\"}"))
	SendMessage(t, 4151, topicName, "pub", []byte("TOBEFAILED"))
	h.messagesSent = 4

	addr := "127.0.0.1:4150"
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
	if !strings.HasPrefix(conn.conn.LocalAddr().String(), laddr) {
		t.Fatal("connection should be bound to the specified address:", conn.conn.LocalAddr())
	}

	err = q.DisconnectFromNSQD("1.2.3.4:4150")
	if err == nil {
		t.Fatal("should not be able to disconnect from an unknown nsqd")
	}

	err = q.ConnectToNSQD("1.2.3.4:4150")
	if err == nil {
		t.Fatal("should not be able to connect to non-existent nsqd")
	}

	err = q.DisconnectFromNSQD("1.2.3.4:4150")
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
