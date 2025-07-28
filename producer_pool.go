package nsq

import (
	"errors"
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

// PublishAsync does a round-robin asynchronous publish. If a publish fails it will enter a retry loop
// to try the next sequential publisher in the pool. The retry loop will be executed for maxPublishRetries
// or the number of publishers in the pool, whichever is smaller. If the message fails to publish after
// exhausting all attempts, the failed transaction will be written to doneChan for handling.
// If successful, the transaction is written to doneChan for handling.
func (p *ProducerPool) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction, args ...interface{}) error {
	if len(p.Producers) < 1 {
		return errors.New("no producers")
	}

	n := int(atomic.AddUint32(&p.next, 1))

	maxAttempts := len(p.Producers)
	if maxAttempts > p.MaxAttempts {
		maxAttempts = p.MaxAttempts
	}

	go func(n int) {
		ch := make(chan *ProducerTransaction, 1)
		defer close(ch)

		for attempt := 0; attempt < maxAttempts; attempt++ {

			isLastAttempt := attempt+1 == maxAttempts

			index := (n + attempt - 1) % len(p.Producers)
			producer := p.Producers[index]

			if err := producer.PublishAsync(topic, body, ch, args...); err != nil {
				if isLastAttempt {
					doneChan <- &ProducerTransaction{Error: err, Args: args}
					break
				}

				producer.log(LogLevelInfo, "(%s) PublishAsync error - %s", producer.conn.String(), err)
				continue
			}

			transaction := <-ch
			if transaction.Error != nil && !isLastAttempt {
				producer.log(LogLevelInfo, "(%s) PublishAsync error - %s", producer.conn.String(), transaction.Error)
				continue
			}
			doneChan <- transaction
			break
		}
	}(n)

	return nil
}
