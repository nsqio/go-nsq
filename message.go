package nsq

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"sync/atomic"
	"time"
)

// The number of bytes for a Message.Id
const MsgIdLength = 16

type MessageID [MsgIdLength]byte

// Message is the fundamental data type containing
// the id, body, and metadata
type Message struct {
	Id        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16

	RequeueCB func(*Message, time.Duration)
	TouchCB   func(*Message)
	FinishCB  func(*Message)

	autoResponseDisabled int32
	responded            int32
}

// NewMessage creates a Message, initializes some metadata,
// and returns a pointer
func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		Id:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

// DisableAutoResponse disables the automatic response that
// would normally be sent when a handler.HandleMessage
// returns (FIN/REQ based on the error value returned).
//
// This is useful if you want to batch, buffer, or asynchronously
// respond to messages.
func (m *Message) DisableAutoResponse() {
	atomic.StoreInt32(&m.autoResponseDisabled, 1)
}

// IsAutoResponseDisabled indicates whether or not this message
// will be responded to automatically
func (m *Message) IsAutoResponseDisabled() bool {
	return atomic.LoadInt32(&m.autoResponseDisabled) == 1
}

// HasResponded indicates whether or not this message has been responded to
func (m *Message) HasResponded() bool {
	return atomic.LoadInt32(&m.responded) == 1
}

// Finish sends a FIN command to the nsqd which
// sent this message
func (m *Message) Finish() {
	if m.HasResponded() {
		return
	}
	m.FinishCB(m)
	atomic.StoreInt32(&m.responded, 1)
}

// Touch sends a TOUCH command to the nsqd which
// sent this message
func (m *Message) Touch() {
	if m.HasResponded() {
		return
	}
	m.TouchCB(m)
}

// Requeue sends a REQ command to the nsqd which
// sent this message, using the supplied delay.
//
// A delay of -1 will automatically calculate
// based on the number of attempts and the
// configured default_requeue_delay
func (m *Message) Requeue(delay time.Duration) {
	m.doRequeue(delay, true)
}

func (m *Message) doRequeue(delay time.Duration, backoff bool) {
	if m.HasResponded() {
		return
	}
	m.RequeueCB(m, delay)
	atomic.StoreInt32(&m.responded, 1)
}

// EncodeBytes serializes the message into a new, returned, []byte
func (m *Message) EncodeBytes() ([]byte, error) {
	var buf bytes.Buffer
	err := m.Write(&buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Write serializes the message into the supplied writer.
//
// It is suggested that the target Writer is buffered to avoid performing many system calls.
func (m *Message) Write(w io.Writer) error {
	err := binary.Write(w, binary.BigEndian, &m.Timestamp)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, &m.Attempts)
	if err != nil {
		return err
	}

	_, err = w.Write(m.Id[:])
	if err != nil {
		return err
	}

	_, err = w.Write(m.Body)
	if err != nil {
		return err
	}

	return nil
}

// DecodeMessage deseralizes data (as []byte) and creates a new Message
func DecodeMessage(byteBuf []byte) (*Message, error) {
	var timestamp int64
	var attempts uint16
	var msg Message

	buf := bytes.NewBuffer(byteBuf)

	err := binary.Read(buf, binary.BigEndian, &timestamp)
	if err != nil {
		return nil, err
	}

	err = binary.Read(buf, binary.BigEndian, &attempts)
	if err != nil {
		return nil, err
	}

	_, err = io.ReadFull(buf, msg.Id[:])
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(buf)
	if err != nil {
		return nil, err
	}

	msg.Body = body
	msg.Timestamp = timestamp
	msg.Attempts = attempts

	return &msg, nil
}
