package queue

import (
	"context"
	"fmt"
	"sync"
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
	ErrFailReachLimits  = fmt.Errorf("fails reaches limits")
)

type Task interface {
	Do() error
	Error() error
	TaskError() error
	ID() string
	Stop()
	Interrupt()
	IsDone() bool
	// The action function when tasks is stop or interrputed.
	OnDone(...Finalizer)
	// The action function when tasks run fail.
	OnFail(...Failure)
	Wait()
	String() string
	IsRunUntilSuccess() bool
	IsReachLimits() bool
	SetTaskError(error)
	TaskContext() *sync.Map
}

type TaskOptions func(*TaskEntry)
type TaskFunc func() error
type RetryFunc func(*TaskEntry) error
type Finalizer func(ok bool, task Task)
type Failure func(fail int, task Task)
type TaskEntry struct {
	id              string
	task            TaskFunc
	taskErr         error
	taskCtx         sync.Map
	retryFunc       RetryFunc
	retryLimit      int
	fails           atomic.Int32
	failLimit       int32
	stop            context.Context
	doStop          context.CancelFunc
	stopOnce        sync.Once
	onTaskDone      []Finalizer
	onTaskFail      []Failure
	running         sync.Mutex
	runUntilSuccess bool
	requireLock     bool
}

func DefaultRetry() RetryFunc {
	return func(tf *TaskEntry) error {
		sleep := time.NewTicker(time.Second)
		defer sleep.Stop()
		var err error
		for i := 0; i < tf.retryLimit; i++ {
			// may be woken up by stop signal or the ticker.
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
		tf.taskErr = err
		return ErrRetryReachLimits
	}
}

// fail fast.
// which promises the task will run once.
func WithNoRetryFunc() TaskOptions {
	return func(te *TaskEntry) {
		te.failLimit = 1
		te.runUntilSuccess = false
		te.retryFunc = func(te *TaskEntry) error {
			if te.taskErr != nil {
				te.Stop()
			}
			return te.taskErr
		}
	}
}

func WithRetryFunc(retry RetryFunc) TaskOptions {
	return func(te *TaskEntry) {
		te.retryFunc = retry
	}
}

func LockRequired() TaskOptions {
	return func(te *TaskEntry) {
		te.requireLock = true
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

func WithOnTaskDone(f Finalizer) TaskOptions {
	return func(te *TaskEntry) {
		te.onTaskDone = append(te.onTaskDone, f)
	}
}

func WithOnTaskFail(f Failure) TaskOptions {
	return func(te *TaskEntry) {
		te.onTaskFail = append(te.onTaskFail, f)
	}
}

func WithFailLimits(limits int) TaskOptions {
	return func(te *TaskEntry) {
		if te.failLimit == DefaultRetryLimit {
			te.failLimit = int32(limits)
		}
	}
}

func NewTask(task TaskFunc, opts ...TaskOptions) Task {
	t := &TaskEntry{
		task:            task,
		runUntilSuccess: true,
		retryLimit:      DefaultRetryLimit,
		retryFunc:       DefaultRetry(),
		failLimit:       DefaultRetryLimit,
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
		currentCnt := t.fails.Add(1)
		if currentCnt > t.failLimit {
			return ErrFailReachLimits
		}
		// most case it wounldn't need
		if t.requireLock {
			t.running.Lock()
			defer t.running.Unlock()
		}
		if err := t.task(); err != nil {
			// saved the error for the fail fast case.
			t.taskErr = err
			if err = t.retryFunc(t); err != nil {
				for _, fail := range t.onTaskFail {
					fail(int(currentCnt), t)
				}
				return err
			}
		}
	}
	t.Stop()
	return nil
}
func (t *TaskEntry) Stop() {
	// prevent the stop race.
	t.stopOnce.Do(func() {
		t.doStop()
		for _, f := range t.onTaskDone {
			f(true, t)
		}
	})

}

func (t *TaskEntry) Interrupt() {
	t.stopOnce.Do(func() {
		t.doStop()
		for _, f := range t.onTaskDone {
			f(false, t)
		}
	})
}

func (t *TaskEntry) SetTaskError(err error) {
	t.taskCtx.Store("err", err)
}

func (t *TaskEntry) TaskError() error {
	if err, ok := t.taskCtx.Load("err"); ok {
		return err.(error)
	}
	return nil
}

func (t *TaskEntry) TaskContext() *sync.Map {
	return &t.taskCtx
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

func (t *TaskEntry) Error() error {
	return t.taskErr
}

func (t *TaskEntry) OnDone(f ...Finalizer) {
	t.onTaskDone = append(t.onTaskDone, f...)
}

func (t *TaskEntry) OnFail(f ...Failure) {
	t.onTaskFail = append(t.onTaskFail, f...)
}

func (t *TaskEntry) Wait() {
	<-t.stop.Done()
}

func (t *TaskEntry) String() string {
	return t.ID()
}

func (t *TaskEntry) IsReachLimits() bool {
	return t.fails.Load() >= t.failLimit
}
