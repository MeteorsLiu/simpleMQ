package worker

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MeteorsLiu/simpleMQ/queue"
)

type Worker struct {
	workerQueue queue.Queue
	pollMap     *queue.PollTask
	sem         chan struct{}
	working     atomic.Int64
	enablePoll  bool
	unlimited   bool
}

func NewWorker(n, spwan int, q queue.Queue, enablePoll ...bool) *Worker {
	w := &Worker{
		workerQueue: q,
		unlimited:   spwan <= 0 || n <= 0,
	}
	if n > 0 {
		w.sem = make(chan struct{}, n)
	}
	if len(enablePoll) > 0 && enablePoll[0] {
		w.enablePoll = true
		w.pollMap = queue.NewPoll()
	}
	for i := 0; i < spwan; i++ {
		w.sem <- struct{}{}
		go w.Run()
	}

	return w
}

func (w *Worker) handleTask(task queue.Task) {
	idx := w.working.Add(1)
	defer w.working.Add(-1)
	var err error
	for {
		err = task.Do()
		if err == nil ||
			err == queue.ErrTaskStopped ||
			err == queue.ErrFailReachLimits ||
			err == queue.ErrQueueClosed ||
			!task.IsRunUntilSuccess() {
			break
		}
		log.Printf("Worker ID: %d Execute Task: %s Fail: %v",
			idx,
			task.ID(),
			err,
		)
		// interrupted by fail fast
		if task.IsDone() {
			return
		}
		log.Printf("Task: %s is going to re-run", task.ID())
		// workerQueue can be nil when worker is in unlimited mode.
		if !w.unlimited && !w.workerQueue.IsClosed() {
			w.workerQueue.Publish(task)
			return
		}
	}

	if err != nil && err != queue.ErrTaskStopped {
		if err == queue.ErrQueueClosed {
			task.Interrupt()
		} else {
			task.Stop()
		}
	}
}

func (w *Worker) Run() {
	defer func() { <-w.sem }()

	for {
		task, err := w.workerQueue.Pop()
		if err != nil {
			return
		}
		if task.IsDone() {
			continue
		}
		w.handleTask(task)
	}
}

func (w *Worker) PublishSync(task queue.Task, callback ...queue.Finalizer) error {
	if w.enablePoll {
		w.pollMap.Register(task)
	}
	task.OnDone(callback...)
	w.handleTask(task)
	return nil
}

func (w *Worker) PublishSyncTimeout(task queue.Task, timeout time.Duration, callback ...queue.Finalizer) error {
	if w.enablePoll {
		w.pollMap.Register(task)
	}
	task.OnDone(callback...)
	timer := time.AfterFunc(timeout, func() {
		task.Stop()
	})
	defer timer.Stop()
	go w.handleTask(task)

	task.Wait()

	return nil
}

func (w *Worker) Publish(task queue.Task, callback ...queue.Finalizer) bool {
	if w.enablePoll {
		w.pollMap.Register(task)
	}

	if w.unlimited {
		task.OnDone(callback...)
		go w.handleTask(task)
		return true
	}

	if w.IsBusy() {
		select {
		case w.sem <- struct{}{}:
			go w.Run()
		default:
		}
	}
	// all the callback function should be registered
	// before it starts to run!

	if len(callback) > 0 {
		task.OnDone(callback...)
	}
	return w.workerQueue.Publish(task)
}

func (w *Worker) Stop() {
	w.Wait(5 * time.Second)
	if w.workerQueue != nil {
		w.workerQueue.Close()
	}
}

func (w *Worker) KillTask(id string) error {
	return w.pollMap.Kill(id)
}

func (w *Worker) Working() int {
	return int(w.working.Load())
}

func (w *Worker) SetQueue(q queue.Queue) {
	w.workerQueue.Close()
	w.workerQueue = q
}

func (w *Worker) IsBusy() bool {
	return w.workerQueue.Free() < cap(w.sem) || w.workerQueue.Len() > w.Working()
}

func (w *Worker) Wait(timeout ...time.Duration) {
	if !w.enablePoll {
		return
	}
	var wg sync.WaitGroup
	w.pollMap.ForEach(func(_ string, p queue.Pollable) bool {
		wg.Add(1)
		go func(task queue.Task) {
			defer wg.Done()
			if len(timeout) > 0 {
				timer := time.AfterFunc(timeout[0], func() {
					task.Interrupt()
				})
				defer timer.Stop()
			}
			task.Wait()
		}(p)
		return true
	})
	wg.Wait()
}
