package nsq

import (
	"sync/atomic"
)

// ProducerPool provides a round robin publish to a pool of producers with built in retry
//
// This includes a transparent retry when a publish fails, and a backoff when
// encountering errors
type ProducerPool struct {
	Producers   []*Producer
	MaxAttempts int
	next        uint32
}

func NewProducerPool(addrs []string, cfg *Config) (*ProducerPool, error) {
	p := &ProducerPool{
		Producers: make([]*Producer, len(addrs)),
	}
	for i, a := range addrs {
		np, err := NewProducer(a, cfg)
		if err != nil {
			return nil, err
		}
		p.Producers[i] = np
	}
	return p, nil
}

// Publish does a round-robin publish, if a publish fails it will retry up to 3 attempts
// and will publish sequentially to the next sequential items in the pool
func (p *ProducerPool) Publish(topic string, body []byte) error {
	n := atomic.AddUint32(&p.next, 1)
	l := len(p.Producers)
	var err error

	for attempt := 0; (attempt <= p.MaxAttempts || p.MaxAttempts <= 0) && attempt < l; attempt++ {
		producer := p.Producers[(int(n)+attempt-1)%l]
		err = producer.Publish(topic, body)
		if err == nil {
			return nil
		}
		producer.log(LogLevelInfo, "(%s) publish error - %s", producer.conn.String(), err)
	}
	return err
}

func (p ProducerPool) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction, args ...interface{}) error {
	panic("not implemented")
}
