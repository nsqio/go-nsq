package nsq_test

import (
	"flag"

	"github.com/bitly/go-nsq"
)

func ExampleConfigFlag() {
	cfg := nsq.NewConfig()
	flagSet := flag.NewFlagSet("", flag.ExitOnError)

	flagSet.Var(&nsq.ConfigFlag{cfg}, "consumer.options", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/bitly/go-nsq#Config.Set)")
	flagSet.PrintDefaults()

	err := flagSet.Parse([]string{
		"-consumer.options=heartbeat_interval,1s",
		"-consumer.options=max_attempts,10",
	})
	if err != nil {
		panic(err.Error())
	}
	println("HeartbeatInterval", cfg.HeartbeatInterval)
	println("MaxAttempts", cfg.MaxAttempts)
}
