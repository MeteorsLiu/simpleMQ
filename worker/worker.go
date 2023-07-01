package worker

import (
	"log"
	"sync/atomic"

	"github.com/MeteorsLiu/simpleMQ/queue"
)

type Worker struct {
	workerQueue queue.Queue
	pollMap     *queue.PollTask
	kill        chan struct{}
	sem         chan struct{}
	working     atomic.Int64
	enablePoll  bool
}

func NewWorker(n, spwan int, q queue.Queue, enablePoll ...bool) *Worker {
	w := &Worker{
		workerQueue: q,
		kill:        make(chan struct{}, n),
		sem:         make(chan struct{}, n),
	}
	if len(enablePoll) > 0 {
		if enablePoll[0] {
			w.enablePoll = true
			w.pollMap = queue.NewPoll()
		}
	}
	for i := 0; i < spwan; i++ {
		w.sem <- struct{}{}
		go w.Run(i + 1)
	}
	return w
}

func (w *Worker) Run(idx int) {
	defer func() { <-w.sem }()
	wq, err := w.workerQueue.Subscribe()
	if err != nil {
		return
	}
	for {
		select {
		case <-w.kill:
			return
		case task, ok := <-wq:
			if !ok {
				return
			}
			if task.IsDone() {
				continue
			}
			w.working.Add(1)
			if err := task.Do(); err != nil {
				log.Printf("Worker ID: %d Execute Task: %s Fail: %v",
					idx,
					task.ID(),
					err,
				)
				if task.IsRunUntilSuccess() {
					if !task.IsDone() && !w.workerQueue.IsClosed() {
						log.Printf("Task: %s is going to re-run", task.ID())
						w.workerQueue.Publish(task)
					}
				} else {
					if err != queue.ErrTaskStopped {
						// run once
						task.Stop()
					}
				}
			}
			w.working.Add(-1)
		}
	}
}

func (w *Worker) Publish(task queue.Task, callback ...func()) bool {
	select {
	case w.sem <- struct{}{}:
		go w.Run(len(w.sem))
	default:
	}
	// all the callback function should be registered
	// before it starts to run!

	if w.enablePoll {
		w.pollMap.Register(task)
	}

	if len(callback) > 0 {
		task.OnDone(callback...)
	}
	return w.workerQueue.Publish(task)
}

func (w *Worker) Killn(n int) {
	for i := 0; i < n; i++ {
		w.kill <- struct{}{}
	}
}

func (w *Worker) Stop() {
	w.Killn(cap(w.sem))
	w.workerQueue.Close()
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
	return w.workerQueue.Free() < cap(w.sem)
}
