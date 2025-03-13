package nsq

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"regexp"
)

// MagicV1 is the initial identifier sent when connecting for V1 clients
var MagicV1 = []byte("  V1")

// MagicV2 is the initial identifier sent when connecting for V2 clients
var MagicV2 = []byte("  V2")

// frame types
const (
	FrameTypeResponse                int32 = 0
	FrameTypeError                   int32 = 1
	FrameTypeMessage                 int32 = 2
	FrameTypeContextCanceled         int32 = 3
	FrameTypeContextDeadlineExceeded int32 = 4
)

// Used to detect if an unexpected HTTP response is read
const httpResponseMsgSize = 1213486160

var validTopicChannelNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+(#ephemeral)?$`)

// IsValidTopicName checks a topic name for correctness
func IsValidTopicName(name string) bool {
	return isValidName(name)
}

// IsValidChannelName checks a channel name for correctness
func IsValidChannelName(name string) bool {
	return isValidName(name)
}

func isValidName(name string) bool {
	if len(name) > 64 || len(name) < 1 {
		return false
	}
	return validTopicChannelNameRegex.MatchString(name)
}

// ReadResponse is a client-side utility function to read from the supplied Reader
// according to the NSQ protocol spec:
//
//	[x][x][x][x][x][x][x][x]...
//	|  (int32) || (binary)
//	|  4-byte  || N-byte
//	------------------------...
//	    size       data
func ReadResponse(r io.Reader, maxMsgSize int32) ([]byte, error) {
	var msgSize int32

	// message size
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	if msgSize < 0 {
		return nil, fmt.Errorf("response msg size is negative: %v", msgSize)
	}

	if maxMsgSize > 0 && msgSize > maxMsgSize {
		if msgSize == httpResponseMsgSize {
			return nil, fmt.Errorf("unexpected HTTP response, a nsqd TCP endpoint is required")
		}
		return nil, fmt.Errorf("response msg size %v exceeds configured maximum (%v)", msgSize, maxMsgSize)
	}

	// message binary data
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// UnpackResponse is a client-side utility function that unpacks serialized data
// according to NSQ protocol spec:
//
//	[x][x][x][x][x][x][x][x]...
//	|  (int32) || (binary)
//	|  4-byte  || N-byte
//	------------------------...
//	  frame ID     data
//
// Returns a triplicate of: frame type, data ([]byte), error
func UnpackResponse(response []byte) (int32, []byte, error) {
	if len(response) < 4 {
		return -1, nil, errors.New("length of response is too small")
	}

	return int32(binary.BigEndian.Uint32(response)), response[4:], nil
}

// ReadUnpackedResponse reads and parses data from the underlying
// TCP connection according to the NSQ TCP protocol spec and
// returns the frameType, data or error
func ReadUnpackedResponse(r io.Reader, maxMsgSize int32) (int32, []byte, error) {
	resp, err := ReadResponse(r, maxMsgSize)
	if err != nil {
		return -1, nil, err
	}
	return UnpackResponse(resp)
}
