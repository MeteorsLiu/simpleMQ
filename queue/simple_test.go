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

func TestTask(t *testing.T) {
	task := NewTask(func() error {
		return nil
	})
	task.TaskContext().Store("t", 123)
	t.Log(task.TaskContext().Load("t"))
}
