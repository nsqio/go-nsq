package nsq

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

// Configure takes an option as a string and a value as an interface and
// attempts to set the appropriate configuration option on the reader instance.
//
// It attempts to coerce the value into the right format depending on the named
// option and the underlying type of the value passed in.
//
// It returns an error for an invalid option or value.
func (q *Reader) Configure(option string, value interface{}) error {
	getDuration := func(v interface{}) (time.Duration, error) {
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

	getBool := func(v interface{}) (bool, error) {
		switch value.(type) {
		case bool:
			return value.(bool), nil
		case string:
			return strconv.ParseBool(v.(string))
		case int, int16, uint16, int32, uint32, int64, uint64:
			return reflect.ValueOf(value).Int() != 0, nil
		}
		return false, errors.New("invalid value type")
	}

	getFloat64 := func(v interface{}) (float64, error) {
		switch value.(type) {
		case string:
			return strconv.ParseFloat(value.(string), 64)
		case int, int16, uint16, int32, uint32, int64, uint64:
			return float64(reflect.ValueOf(value).Int()), nil
		case float64:
			return value.(float64), nil
		}
		return 0, errors.New("invalid value type")
	}

	getInt64 := func(v interface{}) (int64, error) {
		switch value.(type) {
		case string:
			return strconv.ParseInt(v.(string), 10, 64)
		case int, int16, uint16, int32, uint32, int64, uint64:
			return reflect.ValueOf(value).Int(), nil
		}
		return 0, errors.New("invalid value type")
	}

	switch option {
	case "read_timeout":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 5*time.Minute || v < 100*time.Millisecond {
			return errors.New(fmt.Sprintf("invalid %s ! 100ms <= %s <= 5m", option, v))
		}
		q.ReadTimeout = v
	case "write_timeout":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 5*time.Minute || v < 100*time.Millisecond {
			return errors.New(fmt.Sprintf("invalid %s ! 100ms <= %s <= 5m", option, v))
		}
		q.WriteTimeout = v
	case "lookupd_poll_interval":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 5*time.Minute || v < 5*time.Second {
			return errors.New(fmt.Sprintf("invalid %s ! 5s <= %s <= 5m", option, v))
		}
		q.LookupdPollInterval = v
	case "lookupd_poll_jitter":
		v, err := getFloat64(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v < 0 || v > 1 {
			return errors.New(fmt.Sprintf("invalid %s ! 0 <= %v <= 1", option, v))
		}
		q.LookupdPollJitter = v
	case "max_requeue_delay":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 60*time.Minute || v < 0 {
			return errors.New(fmt.Sprintf("invalid %s ! 0 <= %s <= 60m", option, v))
		}
		q.MaxRequeueDelay = v
	case "default_requeue_delay":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 60*time.Minute || v < 0 {
			return errors.New(fmt.Sprintf("invalid %s ! 0 <= %s <= 60m", option, v))
		}
		q.DefaultRequeueDelay = v
	case "backoff_multiplier":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 60*time.Minute || v < time.Second {
			return errors.New(fmt.Sprintf("invalid %s ! 1s <= %s <= 60m", option, v))
		}
		q.BackoffMultiplier = v
	case "max_attempt_count":
		v, err := getInt64(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v < 1 || v > 65535 {
			return errors.New(fmt.Sprintf("invalid %s ! 1 <= %d <= 65535", option, v))
		}
		q.MaxAttemptCount = uint16(v)
	case "low_rdy_idle_timeout":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 5*time.Minute || v < time.Second {
			return errors.New(fmt.Sprintf("invalid %s ! 1s <= %s <= 5m", option, v))
		}
		q.LowRdyIdleTimeout = v
	case "heartbeat_interval":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		q.HeartbeatInterval = v
	case "output_buffer_size":
		v, err := getInt64(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		q.OutputBufferSize = v
	case "output_buffer_timeout":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		q.OutputBufferTimeout = v
	case "tls_v1":
		v, err := getBool(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		q.TLSv1 = v
	case "deflate":
		v, err := getBool(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		q.Deflate = v
	case "deflate_level":
		v, err := getInt64(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v < 1 || v > 9 {
			return errors.New(fmt.Sprintf("invalid %s ! 1 <= %d <= 9", option, v))
		}
		q.DeflateLevel = int(v)
	case "sample_rate":
		v, err := getInt64(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v < 0 || v > 99 {
			return errors.New(fmt.Sprintf("invalid %s ! 0 <= %d <= 99", option, v))
		}
		q.SampleRate = int32(v)
	case "user_agent":
		q.UserAgent = value.(string)
	case "snappy":
		v, err := getBool(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		q.Snappy = v
	case "max_in_flight":
		v, err := getInt64(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v < 1 {
			return errors.New(fmt.Sprintf("invalid %s ! 1 <= %d", option, v))
		}
		q.SetMaxInFlight(int(v))
	case "max_backoff_duration":
		v, err := getDuration(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		if v > 60*time.Minute || v < 0 {
			return errors.New(fmt.Sprintf("invalid %s ! 0 <= %s <= 60m", option, v))
		}
		q.SetMaxBackoffDuration(v)
	case "verbose":
		v, err := getBool(value)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid %s - %s", option, err))
		}
		q.VerboseLogging = v
	}

	return nil
}
