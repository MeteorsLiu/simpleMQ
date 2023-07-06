package queue

import "fmt"

var (
	ErrQueueClosed = fmt.Errorf("queue is closed")
)

type Queue interface {
	IsClosed() bool
	Publish(Task) bool
	ForcePublish(Task) error
	Subscribe() (chan Task, error)
	Close()
	TryPop() (Task, bool)
	Pop() (Task, error)
	Resize(int) bool
	Cap() int
	Len() int
	Free() int
	Copy() []Task
}

type Options func(queue Queue)
type NewQueue func(...Options) Queue
