package queue

import "testing"

func TestSimpleCopy(t *testing.T) {
	q := NewSimpleQueue()
	q.Publish(NewTask(func() error {
		return nil
	}))
	q.Publish(NewTask(func() error {
		return nil
	}))
	q.Publish(NewTask(func() error {
		return nil
	}))
	q.Publish(NewTask(func() error {
		return nil
	}))
	t.Log(q.Copy())
	q.Pop()
	t.Log(q.Copy())
}
