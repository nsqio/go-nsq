package nsq

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/mreiferson/go-snappystream"
	"io"
	"net"
	"sync"
	"time"
)

type nsqConn struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messagesInFlight int64
	messagesReceived uint64
	messagesFinished uint64
	messagesRequeued uint64
	maxRdyCount      int64
	rdyCount         int64
	lastRdyCount     int64
	lastMsgTimestamp int64

	sync.Mutex

	net.Conn
	tlsConn *tls.Conn
	addr    string

	r io.Reader
	w io.Writer

	flateWriter *flate.Writer

	readTimeout  time.Duration
	writeTimeout time.Duration

	backoffCounter int32
	rdyRetryTimer  *time.Timer
	rdyChan        chan *nsqConn

	finishedMessages chan *FinishedMessage
	cmdChan          chan *Command
	dying            chan int
	drainReady       chan int
	exitChan         chan int

	stopFlag int32
	stopper  sync.Once
}

func newNSQConn(rdyChan chan *nsqConn, addr string,
	readTimeout time.Duration, writeTimeout time.Duration) (*nsqConn, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return nil, err
	}

	nc := &nsqConn{
		Conn: conn,

		addr: addr,

		r: bufio.NewReader(conn),
		w: conn,

		readTimeout:      readTimeout,
		writeTimeout:     writeTimeout,
		maxRdyCount:      2500,
		lastMsgTimestamp: time.Now().UnixNano(),

		finishedMessages: make(chan *FinishedMessage),
		cmdChan:          make(chan *Command),
		dying:            make(chan int, 1),
		drainReady:       make(chan int),
		rdyChan:          rdyChan,
		exitChan:         make(chan int),
	}

	_, err = nc.Write(MagicV2)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("[%s] failed to write magic - %s", addr, err.Error())
	}

	return nc, nil
}

func (c *nsqConn) String() string {
	return c.addr
}

func (c *nsqConn) Read(p []byte) (int, error) {
	c.SetReadDeadline(time.Now().Add(c.readTimeout))
	return c.r.Read(p)
}

func (c *nsqConn) Write(p []byte) (int, error) {
	c.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	return c.w.Write(p)
}

func (c *nsqConn) sendCommand(buf *bytes.Buffer, cmd *Command) error {
	c.Lock()
	defer c.Unlock()

	buf.Reset()
	err := cmd.Write(buf)
	if err != nil {
		return err
	}

	_, err = buf.WriteTo(c)
	if err != nil {
		return err
	}

	if c.flateWriter != nil {
		return c.flateWriter.Flush()
	}

	return nil
}

func (c *nsqConn) readUnpackedResponse() (int32, []byte, error) {
	resp, err := ReadResponse(c)
	if err != nil {
		return -1, nil, err
	}
	return UnpackResponse(resp)
}

func (c *nsqConn) upgradeTLS(conf *tls.Config) error {
	c.tlsConn = tls.Client(c.Conn, conf)
	err := c.tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.r = bufio.NewReader(c.tlsConn)
	c.w = c.tlsConn
	frameType, data, err := c.readUnpackedResponse()
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from TLS upgrade")
	}
	return nil
}

func (c *nsqConn) upgradeDeflate(level int) error {
	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}
	c.r = bufio.NewReader(flate.NewReader(conn))
	fw, _ := flate.NewWriter(conn, level)
	c.flateWriter = fw
	c.w = fw
	frameType, data, err := c.readUnpackedResponse()
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from Deflate upgrade")
	}
	return nil
}

func (c *nsqConn) upgradeSnappy() error {
	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}
	c.r = bufio.NewReader(snappystream.NewReader(conn, snappystream.SkipVerifyChecksum))
	c.w = snappystream.NewWriter(conn)
	frameType, data, err := c.readUnpackedResponse()
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from Snappy upgrade")
	}
	return nil
}
