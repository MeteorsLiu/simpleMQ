package queue

import (
	"sync"

	"github.com/MeteorsLiu/simpleMQ/generic"
)

type FairQueue struct {
	sync.RWMutex
	cap   int
	queue *generic.List[Task]
}

func WithFairQueueCap(cap int) Options {
	return func(queue Queue) {
		queue.Resize(cap)
	}
}

func NewFairQueue(opts ...Options) Queue {
	fq := &FairQueue{
		queue: generic.New[Task](),
	}
	for _, o := range opts {
		o(fq)
	}

	return fq
}

func (f *FairQueue) Resize(cap int) bool {
	return true
}

func (f *FairQueue) Cap() int {
	f.RLock()
	defer f.RUnlock()
	return f.cap
}
func (f *FairQueue) Len() int {
	f.RLock()
	defer f.RUnlock()
	return f.cap
}

func (f *FairQueue) IsClosed() bool {
	return false
}

// close wait until all tasks are executed.
func (f *FairQueue) Close() {

}

func (f *FairQueue) Publish(t Task) bool {
	return false
}

func (f *FairQueue) ForcePublish(t Task) error {
	return nil
}
func (f *FairQueue) Subscribe() (chan Task, error) {
	return nil, nil
}
func (f *FairQueue) TryPop() (Task, bool) {
	return nil, false
}

func (f *FairQueue) Pop() (Task, error) {
	return nil, nil
}
