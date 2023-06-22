package queue

import (
	"sync"
)

const (
	DefaultSqSize = 1024
)

type SimpleQueue struct {
	closeRW   sync.RWMutex
	isClosed  bool
	taskQueue chan Task
}

func WithSimpleQueueCap(cap int) Options {
	return func(queue Queue) {
		queue.(*SimpleQueue).taskQueue = make(chan Task, cap)
	}
}

func NewSimpleQueue(opts ...Options) Queue {
	s := &SimpleQueue{}
	for _, o := range opts {
		o(s)
	}
	if cap(s.taskQueue) == 0 {
		s.taskQueue = make(chan Task, DefaultSqSize)
	}
	return s
}
func (s *SimpleQueue) Resize(capSize int) bool {
	// stop executing all the new tasks
	s.closeRW.Lock()
	defer s.closeRW.Unlock()
	if s.isClosed {
		return false
	}
	// initialize or fast resize
	if cap(s.taskQueue) == 0 || len(s.taskQueue) == 0 {
		s.taskQueue = make(chan Task, capSize)
		return true
	}
	// a hot resize
	newTaskQueue := make(chan Task, capSize)
	defer func() {
		s.taskQueue = newTaskQueue
	}()
	for {
		select {
		case task := <-s.taskQueue:
			select {
			case newTaskQueue <- task:
			default:
				return true
			}
		default:
			return true
		}
	}
}

func (s *SimpleQueue) Cap() int {
	return cap(s.taskQueue)
}
func (s *SimpleQueue) Len() int {
	return len(s.taskQueue)
}

func (s *SimpleQueue) IsClosed() bool {
	s.closeRW.RLock()
	defer s.closeRW.RUnlock()
	return s.isClosed
}

// close wait until all tasks are executed.
func (s *SimpleQueue) Close() {
	s.closeRW.Lock()
	s.isClosed = false
	// wake up the subscribers
	close(s.taskQueue)
	s.closeRW.Unlock()
}

func (s *SimpleQueue) Publish(t Task) bool {
	s.closeRW.RLock()
	defer s.closeRW.RUnlock()
	if s.isClosed {
		return false
	}
	select {
	case s.taskQueue <- t:
	default:
		return false
	}
	return true
}

func (s *SimpleQueue) ForcePublish(t Task) error {
	s.closeRW.RLock()
	defer s.closeRW.RUnlock()
	if s.isClosed {
		return ErrQueueClosed
	}
	s.taskQueue <- t
	return nil
}
func (s *SimpleQueue) Subscribe() (chan Task, error) {
	s.closeRW.RLock()
	defer s.closeRW.RUnlock()
	if s.isClosed {
		return nil, ErrQueueClosed
	}
	return s.taskQueue, nil
}
func (s *SimpleQueue) TryPop() (Task, bool) {
	s.closeRW.RLock()
	defer s.closeRW.RUnlock()
	if s.isClosed {
		return nil, false
	}
	select {
	case t := <-s.taskQueue:
		return t, true
	default:
	}
	return nil, false
}

func (s *SimpleQueue) Pop() (Task, error) {
	s.closeRW.RLock()
	defer s.closeRW.RUnlock()
	if s.isClosed {
		return nil, ErrQueueClosed
	}
	task := <-s.taskQueue
	return task, nil
}
