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
}
type TaskOptions func(*TaskEntry)
type TaskFunc func() error
type RetryFunc func(*TaskEntry) error

type TaskEntry struct {
	id              string
	task            TaskFunc
	taskErr         error
	retryFunc       RetryFunc
	retryLimit      int
	stop            context.Context
	doStop          context.CancelFunc
	done            atomic.Bool
	runUntilSuccess bool
}

func DefaultRetry() RetryFunc {
	return func(tf *TaskEntry) error {
		sleep := time.NewTicker(time.Second)
		defer sleep.Stop()

		for i := 0; i < tf.retryLimit; i++ {

			// when multi channels are available,
			// the go runtime will select it randomly.
			// however, we must ensure the stop signal is first,
			// otherwise, this goroutine will be paused by the ticker channel.
			select {
			case <-tf.stop.Done():
				return ErrTaskStopped
			default:
			}

			// may be paused
			select {
			case <-tf.stop.Done():
				return ErrTaskStopped
			case <-sleep.C:
			}

			if err := tf.task(); err == nil {
				return nil
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

func NewTask(task TaskFunc, opts ...TaskOptions) Task {
	t := &TaskEntry{
		task:            task,
		runUntilSuccess: true,
	}
	for _, o := range opts {
		o(t)
	}
	if t.retryLimit == 0 {
		t.retryLimit = DefaultRetryLimit
	}
	if t.retryFunc == nil {
		t.retryFunc = DefaultRetry()
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
	t.done.Store(true)
	return nil
}
func (t *TaskEntry) Stop() {
	t.doStop()
}

func (t *TaskEntry) ID() string {
	return t.id
}

func (t *TaskEntry) IsRunUntilSuccess() bool {
	return t.runUntilSuccess
}

func (t *TaskEntry) IsDone() bool {
	return t.done.Load()
}
