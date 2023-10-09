package broker

import (
	"github.com/dylanbr0wn/pbsb/gen/pbsb"
)

type Queue struct {
	BuffChan chan *pbsb.Message
}

func (q *Queue) Enqueue(msg *pbsb.Message) {
	q.BuffChan <- msg
}

func (q *Queue) Dequeue() *pbsb.Message {
	return <-q.BuffChan
}

func (q *Queue) Channel() chan *pbsb.Message {
	return q.BuffChan
}
