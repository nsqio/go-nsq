package nsq

import (
	"fmt"
	"log"
	"os"
	"sync"
)

type LoggerCarrier interface {
	GetLogLevel() LogLevel
	GetLoggers() []logger
	SetLogger(l logger, lvl LogLevel, format string)
	SetLoggerForLevel(l logger, lvl LogLevel, format string)
	SetLoggerLevel(lvl LogLevel)
	Log(lvl LogLevel, line string, addr string, args ...interface{})
}

type DefaultLoggerCarrier struct {
	LoggerCarrier

	logger []logger
	logLvl LogLevel
	logFmt []string

	logGuard sync.RWMutex
}

func NewDefaultLoggerCarrier() LoggerCarrier {
	carrier := &DefaultLoggerCarrier{
		logger: make([]logger, int(LogLevelMax+1)),
		logLvl: LogLevelInfo,
		logFmt: make([]string, LogLevelMax+1),
	}

	l := log.New(os.Stderr, "", log.Flags())
	for index := range carrier.logger {
		carrier.logger[index] = l
	}

	return carrier
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (c *DefaultLoggerCarrier) SetLogger(l logger, lvl LogLevel, format string) {
	c.logGuard.Lock()
	defer c.logGuard.Unlock()

	if format == "" {
		format = "(%s)"
	}
	for level := range c.logger {
		c.logger[level] = l
		c.logFmt[level] = format
	}
	c.logLvl = lvl
}

func (c *DefaultLoggerCarrier) SetLoggerForLevel(l logger, lvl LogLevel, format string) {
	c.logGuard.Lock()
	defer c.logGuard.Unlock()

	if format == "" {
		format = "(%s)"
	}
	c.logger[lvl] = l
	c.logFmt[lvl] = format
}

func (c *DefaultLoggerCarrier) SetLoggerLevel(lvl LogLevel) {
	c.logGuard.Lock()
	defer c.logGuard.Unlock()

	c.logLvl = lvl
}

func (c *DefaultLoggerCarrier) GetLoggers() []logger {
	c.logGuard.RLock()
	defer c.logGuard.RUnlock()

	return c.logger
}

func (c *DefaultLoggerCarrier) GetLogLevel() LogLevel {
	c.logGuard.RLock()
	defer c.logGuard.RUnlock()

	return c.logLvl
}

func (c *DefaultLoggerCarrier) Log(
	lvl LogLevel,
	line string,
	addr string,
	args ...interface{},
) {
	logger, logLvl, logFmt := c.logger[lvl], c.logLvl, c.logFmt[lvl]

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(
		2,
		fmt.Sprintf(
			"%-4s %s %s",
			lvl,
			fmt.Sprintf(logFmt, addr),
			fmt.Sprintf(line, args...),
		),
	)
}

type logger interface {
	Output(calldepth int, s string) error
}

// LogLevel specifies the severity of a given log message
type LogLevel int

// Log levels
const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarning
	LogLevelError
	LogLevelMax = iota - 1 // convenience - match highest log level
)

// String returns the string form for a given LogLevel
func (lvl LogLevel) String() string {
	switch lvl {
	case LogLevelInfo:
		return "INF"
	case LogLevelWarning:
		return "WRN"
	case LogLevelError:
		return "ERR"
	}
	return "DBG"
}
