package worker

import (
	"log"
	"sync/atomic"

	"github.com/MeteorsLiu/simpleMQ/queue"
)

type Worker struct {
	workerQueue queue.Queue
	pollMap     *queue.PollTask
	sem         chan struct{}
	working     atomic.Int64
	enablePoll  bool
}

func NewWorker(n, spwan int, q queue.Queue, enablePoll ...bool) *Worker {
	w := &Worker{
		workerQueue: q,
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

func (w *Worker) handleTask(idx int, task queue.Task) {
	w.working.Add(1)
	defer w.working.Add(-1)

	if err := task.Do(); err != nil && err != queue.ErrTaskStopped {
		log.Printf("Worker ID: %d Execute Task: %s Fail: %v",
			idx,
			task.ID(),
			err,
		)
		if task.IsRunUntilSuccess() &&
			!w.workerQueue.IsClosed() {
			log.Printf("Task: %s is going to re-run", task.ID())
			w.workerQueue.Publish(task)
			return
		}
	}

	task.Stop()
}

func (w *Worker) Run(idx int) {
	defer func() { <-w.sem }()

	for {
		task, err := w.workerQueue.Pop()
		if err != nil {
			return
		}
		if task.IsDone() {
			continue
		}
		w.handleTask(idx, task)
	}
}

func (w *Worker) Publish(task queue.Task, callback ...func()) bool {
	if w.IsBusy() {
		select {
		case w.sem <- struct{}{}:
			go w.Run(len(w.sem))
		default:
		}
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

func (w *Worker) Stop() {
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
	return w.workerQueue.Free() < cap(w.sem) || w.workerQueue.Len() > w.Working()
}
