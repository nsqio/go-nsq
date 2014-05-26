package nsq

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// Config is a struct of NSQ options
//
// (see Config.Set() for available parameters)
type Config struct {
	sync.RWMutex

	verbose bool `opt:"verbose"`

	readTimeout  time.Duration `opt:"read_timeout" min:"100ms" max:"5m"`
	writeTimeout time.Duration `opt:"write_timeout" min:"100ms" max:"5m"`

	lookupdPollInterval time.Duration `opt:"lookupd_poll_interval" min:"5s" max:"5m"`
	lookupdPollJitter   float64       `opt:"lookupd_poll_jitter" min:"0" max:"1"`

	maxRequeueDelay     time.Duration `opt:"max_requeue_delay" min:"0" max:"60m"`
	defaultRequeueDelay time.Duration `opt:"default_requeue_delay" min:"0" max:"60m"`
	backoffMultiplier   time.Duration `opt:"backoff_multiplier" min:"0" max:"60m"`

	maxAttempts       uint16        `opt:"max_attempts" min:"1" max:"65535"`
	lowRdyIdleTimeout time.Duration `opt:"low_rdy_idle_timeout" min:"1s" max:"5m"`

	clientID  string `opt:"client_id"`
	hostname  string `opt:"hostname"`
	userAgent string `opt:"user_agent"`

	heartbeatInterval time.Duration `opt:"heartbeat_interval"`
	sampleRate        int32         `opt:"sample_rate" min:"0" max:"99"`

	tlsV1     bool        `opt:"tls_v1"`
	tlsConfig *tls.Config `opt:"tls_config"`

	deflate      bool `opt:"deflate"`
	deflateLevel int  `opt:"deflate_level" min:"1" max:"9"`
	snappy       bool `opt:"snappy"`

	outputBufferSize    int64         `opt:"output_buffer_size"`
	outputBufferTimeout time.Duration `opt:"output_buffer_timeout"`

	maxInFlight      int `opt:"max_in_flight" min:"0"`
	maxInFlightMutex sync.RWMutex

	maxBackoffDuration time.Duration `opt:"max_backoff_duration" min:"0" max:"60m"`
}

// NewConfig returns a new default configuration
func NewConfig() *Config {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	}
	conf := &Config{
		maxInFlight: 1,
		maxAttempts: 5,

		lookupdPollInterval: 60 * time.Second,
		lookupdPollJitter:   0.3,

		lowRdyIdleTimeout: 10 * time.Second,

		defaultRequeueDelay: 90 * time.Second,
		maxRequeueDelay:     15 * time.Minute,

		backoffMultiplier:  time.Second,
		maxBackoffDuration: 120 * time.Second,

		readTimeout:  DefaultClientTimeout,
		writeTimeout: time.Second,

		deflateLevel:        6,
		outputBufferSize:    16 * 1024,
		outputBufferTimeout: 250 * time.Millisecond,

		heartbeatInterval: DefaultClientTimeout / 2,

		clientID:  strings.Split(hostname, ".")[0],
		hostname:  hostname,
		userAgent: fmt.Sprintf("go-nsq/%s", VERSION),
	}
	return conf
}

// Set takes an option as a string and a value as an interface and
// attempts to set the appropriate configuration option.
//
// It attempts to coerce the value into the right format depending on the named
// option and the underlying type of the value passed in.
//
// It returns an error for an invalid option or value.
//
// 	verbose: enable verbose logging
//
// 	read_timeout: the deadline set for network reads
//
// 	write_timeout: the deadline set for network writes
//
// 	lookupd_poll_interval: duration between polling lookupd for new
//
// 	lookupd_poll_jitter: fractional amount of jitter to add to the lookupd pool loop,
// 	                     this helps evenly distribute requests even if multiple
// 	                     consumers restart at the same time.
//
// 	max_requeue_delay: the maximum duration when REQueueing (for doubling of deferred requeue)
//
// 	default_requeue_delay: the default duration when REQueueing
//
// 	backoff_multiplier: the unit of time for calculating consumer backoff
//
// 	max_attempts: maximum number of times this consumer will attempt to process a message
//
// 	low_rdy_idle_timeout: the amount of time in seconds to wait for a message from a producer
// 	                      when in a state where RDY counts are re-distributed
// 	                      (ie. max_in_flight < num_producers)
//
// 	client_id: an identifier sent to nsqd representing the client (defaults: short hostname)
//
// 	hostname: an identifier sent to nsqd representing the host (defaults: long hostname)
//
// 	user_agent: a string identifying the agent for this client (in the spirit of HTTP)
// 	            (default: "<client_library_name>/<version>")
//
// 	heartbeat_interval: duration of time between heartbeats
//
// 	sample_rate: integer percentage to sample the channel (requires nsqd 0.2.25+)
//
// 	tls_v1: negotiate TLS
//
// 	tls_config: client TLS configuration
//
// 	deflate: negotiate Deflate compression
//
// 	deflate_level: the compression level to negotiate for Deflate
//
// 	snappy: negotiate Snappy compression
//
// 	output_buffer_size: size of the buffer (in bytes) used by nsqd for
// 	                    buffering writes to this connection
//
// 	output_buffer_timeout: timeout (in ms) used by nsqd before flushing buffered
// 	                       writes (set to 0 to disable).
//
// 	                       WARNING: configuring clients with an extremely low
// 	                       (< 25ms) output_buffer_timeout has a significant effect
// 	                       on nsqd CPU usage (particularly with > 50 clients connected).
//
// 	max_in_flight: the maximum number of messages to allow in flight (concurrency knob)
//
// 	max_backoff_duration: the maximum amount of time to backoff when processing fails
// 	                      0 == no backoff
//
func (c *Config) Set(option string, value interface{}) error {
	c.Lock()
	defer c.Unlock()

	val := reflect.ValueOf(c).Elem()
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		opt := field.Tag.Get("opt")
		min := field.Tag.Get("min")
		max := field.Tag.Get("max")

		if option != opt {
			continue
		}

		fieldVal := val.FieldByName(field.Name)
		dest := unsafeValueOf(fieldVal)
		coercedVal, err := coerce(value, field.Type)
		if err != nil {
			log.Fatalf("ERROR: failed to coerce option %s (%v) - %s",
				option, value, err)
		}
		if min != "" {
			coercedMinVal, _ := coerce(min, field.Type)
			if valueCompare(coercedVal, coercedMinVal) == -1 {
				return errors.New(fmt.Sprintf("invalid %s ! %v < %v",
					option, coercedVal.Interface(), coercedMinVal.Interface()))
			}
		}
		if max != "" {
			coercedMaxVal, _ := coerce(max, field.Type)
			if valueCompare(coercedVal, coercedMaxVal) == 1 {
				return errors.New(fmt.Sprintf("invalid %s ! %v > %v",
					option, coercedVal.Interface(), coercedMaxVal.Interface()))
			}
		}
		dest.Set(coercedVal)
	}

	return errors.New(fmt.Sprintf("ERROR: invalid option %s", option))
}

// because Config contains private structs we can't use reflect.Value
// directly, instead we need to "unsafely" address the variable
func unsafeValueOf(val reflect.Value) reflect.Value {
	uptr := unsafe.Pointer(val.UnsafeAddr())
	return reflect.NewAt(val.Type(), uptr).Elem()
}

func valueCompare(v1 reflect.Value, v2 reflect.Value) int {
	switch v1.Type().String() {
	case "int", "uint", "int16", "uint16", "int32", "uint32", "int64", "uint64":
		if v1.Int() > v2.Int() {
			return 1
		} else if v1.Int() < v2.Int() {
			return -1
		}
		return 0
	case "float32", "float64":
		if v1.Float() > v2.Float() {
			return 1
		} else if v1.Float() < v2.Float() {
			return -1
		}
		return 0
	case "time.Duration":
		if v1.Interface().(time.Duration) > v2.Interface().(time.Duration) {
			return 1
		} else if v1.Interface().(time.Duration) < v2.Interface().(time.Duration) {
			return -1
		}
		return 0
	}
	panic("impossible")
}

func coerce(v interface{}, typ reflect.Type) (reflect.Value, error) {
	var err error
	if typ.Kind() == reflect.Ptr {
		return reflect.ValueOf(v), nil
	}
	switch typ.String() {
	case "string":
		v, err = coerceString(v)
	case "int", "uint", "int16", "uint16", "int32", "uint32", "int64", "uint64":
		v, err = coerceInt64(v)
	case "float32", "float64":
		v, err = coerceFloat64(v)
	case "bool":
		v, err = coerceBool(v)
	case "time.Duration":
		v, err = coerceDuration(v)
	default:
		v = nil
		err = errors.New(fmt.Sprintf("invalid type %s", typ.String()))
	}
	return valueTypeCoerce(v, typ), err
}

func valueTypeCoerce(v interface{}, typ reflect.Type) reflect.Value {
	val := reflect.ValueOf(v)
	if reflect.TypeOf(v) == typ {
		return val
	}
	tval := reflect.New(typ).Elem()
	switch typ.String() {
	case "int", "uint", "int16", "uint16", "int32", "uint32", "int64", "uint64":
		tval.SetInt(val.Int())
	case "float32", "float64":
		tval.SetFloat(val.Float())
	}
	return tval
}

func coerceString(v interface{}) (string, error) {
	switch v.(type) {
	case string:
		return v.(string), nil
	case int, int16, uint16, int32, uint32, int64, uint64:
		return fmt.Sprintf("%d", v), nil
	case float64:
		return fmt.Sprintf("%f", v), nil
	default:
		return fmt.Sprintf("%s", v), nil
	}
	return "", errors.New("invalid value type")
}

func coerceDuration(v interface{}) (time.Duration, error) {
	switch v.(type) {
	case string:
		return time.ParseDuration(v.(string))
	case int, int16, uint16, int32, uint32, int64, uint64:
		// treat like ms
		return time.Duration(reflect.ValueOf(v).Int()) * time.Millisecond, nil
	case time.Duration:
		return v.(time.Duration), nil
	}
	return 0, errors.New("invalid value type")
}

func coerceBool(v interface{}) (bool, error) {
	switch v.(type) {
	case bool:
		return v.(bool), nil
	case string:
		return strconv.ParseBool(v.(string))
	case int, int16, uint16, int32, uint32, int64, uint64:
		return reflect.ValueOf(v).Int() != 0, nil
	}
	return false, errors.New("invalid value type")
}

func coerceFloat64(v interface{}) (float64, error) {
	switch v.(type) {
	case string:
		return strconv.ParseFloat(v.(string), 64)
	case int, int16, uint16, int32, uint32, int64, uint64:
		return float64(reflect.ValueOf(v).Int()), nil
	case float64:
		return v.(float64), nil
	}
	return 0, errors.New("invalid value type")
}

func coerceInt64(v interface{}) (int64, error) {
	switch v.(type) {
	case string:
		return strconv.ParseInt(v.(string), 10, 64)
	case int, int16, uint16, int32, uint32, int64, uint64:
		return reflect.ValueOf(v).Int(), nil
	}
	return 0, errors.New("invalid value type")
}
