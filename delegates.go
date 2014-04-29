package nsq

import "time"

// LogLevel specifies the severity of a given log message
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarning
	LogLevelError
)

// MessageDelegate is an interface of methods that are used as
// callbacks in Message
type MessageDelegate interface {
	// OnFinish is called when the Finish() method
	// is triggered on the Message
	OnFinish(*Message)

	// OnRequeue is called when the Requeue() method
	// is triggered on the Message
	OnRequeue(*Message, time.Duration)

	// OnTouch is called when the Touch() method
	// is triggered on the Message
	OnTouch(*Message)
}

type connMessageDelegate struct {
	c *Conn
}

func (d *connMessageDelegate) OnFinish(m *Message)                   { d.c.onMessageFinish(m) }
func (d *connMessageDelegate) OnRequeue(m *Message, t time.Duration) { d.c.onMessageRequeue(m, t) }
func (d *connMessageDelegate) OnTouch(m *Message)                    { d.c.onMessageTouch(m) }

type ConnDelegate interface {
	// OnResponse is called when the connection
	// receives a FrameTypeResponse from nsqd
	OnResponse(*Conn, []byte)

	// OnError is called when the connection
	// receives a FrameTypeError from nsqd
	OnError(*Conn, []byte)

	// OnMessage is called when the connection
	// receives a FrameTypeMessage from nsqd
	OnMessage(*Conn, *Message)

	// OnMessageFinished is called when the connection
	// handles a FIN command from a message handler
	OnMessageFinished(*Conn, *Message)

	// OnMessageRequeued is called when the connection
	// handles a REQ command from a message handler
	OnMessageRequeued(*Conn, *Message)

	// OnIOError is called when the connection experiences
	// a low-level TCP transport error
	OnIOError(*Conn, error)

	// OnHeartbeat is called when the connection
	// receives a heartbeat from nsqd
	OnHeartbeat(*Conn)

	// OnClose is called when the connection
	// closes, after all cleanup
	OnClose(*Conn)
}

// keeps the exported Reader struct clean of the exported methods
// required to implement the ConnDelegate interface
type readerConnDelegate struct {
	r *Reader
}

func (d *readerConnDelegate) OnResponse(c *Conn, data []byte)       { d.r.onConnResponse(c, data) }
func (d *readerConnDelegate) OnError(c *Conn, data []byte)          { d.r.onConnError(c, data) }
func (d *readerConnDelegate) OnMessage(c *Conn, m *Message)         { d.r.onConnMessage(c, m) }
func (d *readerConnDelegate) OnMessageFinished(c *Conn, m *Message) { d.r.onConnMessageFinished(c, m) }
func (d *readerConnDelegate) OnMessageRequeued(c *Conn, m *Message) { d.r.onConnMessageRequeued(c, m) }
func (d *readerConnDelegate) OnIOError(c *Conn, err error)          { d.r.onConnIOError(c, err) }
func (d *readerConnDelegate) OnHeartbeat(c *Conn)                   { d.r.onConnHeartbeat(c) }
func (d *readerConnDelegate) OnClose(c *Conn)                       { d.r.onConnClose(c) }

// keeps the exported Writer struct clean of the exported methods
// required to implement the ConnDelegate interface
type writerConnDelegate struct {
	w *Writer
}

func (d *writerConnDelegate) OnResponse(c *Conn, data []byte)       { d.w.onConnResponse(c, data) }
func (d *writerConnDelegate) OnError(c *Conn, data []byte)          { d.w.onConnError(c, data) }
func (d *writerConnDelegate) OnMessage(c *Conn, m *Message)         {}
func (d *writerConnDelegate) OnMessageFinished(c *Conn, m *Message) {}
func (d *writerConnDelegate) OnMessageRequeued(c *Conn, m *Message) {}
func (d *writerConnDelegate) OnIOError(c *Conn, err error)          { d.w.onConnIOError(c, err) }
func (d *writerConnDelegate) OnHeartbeat(c *Conn)                   { d.w.onConnHeartbeat(c) }
func (d *writerConnDelegate) OnClose(c *Conn)                       { d.w.onConnClose(c) }
