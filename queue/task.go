package queue

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultRetryLimit = 5
)

var (
	ErrTaskStopped      = fmt.Errorf("task is stopped")
	ErrRetryReachLimits = fmt.Errorf("retry reaches limits")
)

type Task interface {
	Do() error
	ID() string
	IsRunUntilSuccess() bool
	Stop()
	IsDone() bool
	OnDone(...func())
}
type TaskOptions func(*TaskEntry)
type TaskFunc func() error
type RetryFunc func(*TaskEntry) error

type TaskEntry struct {
	id              string
	task            TaskFunc
	taskErr         error
	taskRunning     atomic.Int32
	preemptSig      chan struct{}
	canPreempt      bool
	ptrPreemptM     chan uintptr
	retryFunc       RetryFunc
	retryLimit      int
	stop            context.Context
	doStop          context.CancelFunc
	onTaskDone      []func()
	runUntilSuccess bool
}

func DefaultRetry() RetryFunc {
	return func(tf *TaskEntry) error {
		tf.taskRunning.Add(1)
		defer tf.taskRunning.Add(-1)
		sleep := time.NewTicker(time.Second)
		defer sleep.Stop()
		errCh := make(chan string)

		for i := 0; i < tf.retryLimit; i++ {
			// may be woken up by stop signal or the ticker.
			select {
			case <-tf.stop.Done():
				return ErrTaskStopped
			case <-sleep.C:
			}

			go func() {
				if tf.canPreempt {

				}
				if err := tf.task(); err == nil {
					errCh <- ""
				} else {
					errCh <- err.Error()
				}
			}()

			select {
			case err := <-errCh:
				if err == "" {
					return nil
				}
			case <-tf.preemptSig:
			}

			// max retry sleep:
			// first time: 1 seconds
			// and then 2 seconds,  4 seconds,  8 seconds,  16 seconds, 32 seconds.
			// Totally, 1+2+4+8+16+32=63 seconds = 1 Minute 3 Second
			sleep.Reset(1 << (i + 1) * time.Second)
		}
		return ErrRetryReachLimits
	}
}

// fail fast.
func WithNoRetryFunc() TaskOptions {
	return func(te *TaskEntry) {
		te.retryFunc = func(te *TaskEntry) error {
			return te.taskErr
		}
	}
}

func WithRetryFunc(retry RetryFunc) TaskOptions {
	return func(te *TaskEntry) {
		te.retryFunc = retry
		te.canPreempt = false
	}
}

// when the custom RetryFunc is set,
// the retry limit will be ignored.
func WithRetryLimit(retry int) TaskOptions {
	return func(te *TaskEntry) {
		te.retryLimit = retry
	}
}

func WithContext(ctx context.Context) TaskOptions {
	return func(te *TaskEntry) {
		te.stop, te.doStop = context.WithCancel(ctx)
	}
}

func WithTaskID(id string) TaskOptions {
	return func(te *TaskEntry) {
		te.id = id
	}
}

func WithRunUntilSuccess(RunUntilSuccess bool) TaskOptions {
	return func(te *TaskEntry) {
		te.runUntilSuccess = RunUntilSuccess
	}
}

func WithOnTaskDone(f func()) TaskOptions {
	return func(te *TaskEntry) {
		te.onTaskDone = append(te.onTaskDone, f)
	}
}

func NewTask(task TaskFunc, opts ...TaskOptions) Task {
	t := &TaskEntry{
		task:            task,
		runUntilSuccess: true,
		canPreempt:      true,
		ptrPreemptM:     make(chan uintptr, 1),
		retryLimit:      DefaultRetryLimit,
		retryFunc:       DefaultRetry(),
	}

	for _, o := range opts {
		o(t)
	}
	if t.stop == nil || t.doStop == nil {
		t.stop, t.doStop = context.WithCancel(context.Background())
	}
	if t.id == "" {
		t.id = uuid.NewString()
	}
	return t
}

func (t *TaskEntry) Do() error {
	select {
	case <-t.stop.Done():
		return ErrTaskStopped
	default:
		if err := t.task(); err != nil {
			// saved the error for the fail fast case.
			t.taskErr = err
			if err = t.retryFunc(t); err != nil {
				return err
			}
		}
	}
	t.Stop()
	return nil
}
func (t *TaskEntry) Stop() {
	for _, f := range t.onTaskDone {
		f()
	}
	t.doStop()
}

func (t *TaskEntry) ID() string {
	return t.id
}

func (t *TaskEntry) IsRunUntilSuccess() bool {
	return t.runUntilSuccess
}

func (t *TaskEntry) IsDone() bool {
	select {
	case <-t.stop.Done():
		return true
	default:
		return false
	}
}

func (t *TaskEntry) OnDone(f ...func()) {
	t.onTaskDone = append(t.onTaskDone, f...)
}
