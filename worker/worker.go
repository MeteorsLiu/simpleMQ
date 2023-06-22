package worker

import (
	"log"
	"sync/atomic"

	"github.com/MeteorsLiu/simpleMQ/queue"
)

type Worker struct {
	workerQueue queue.Queue
	kill        chan struct{}
	sem         chan struct{}
	working     atomic.Int64
}

func NewWorker(n int, queue queue.Queue) *Worker {
	w := &Worker{
		workerQueue: queue,
		kill:        make(chan struct{}, n),
		sem:         make(chan struct{}, n),
	}
	for i := 0; i < n; i++ {
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
			w.working.Add(1)
			if err := task.Do(); err != nil {
				log.Printf("Worker ID: %d Execute Task: %s Fail: %v",
					idx,
					task.ID(),
					err,
				)
				if task.IsRunUntilSuccess() {
					log.Printf("Task: %s is going to re-run", task.ID())
					if !w.workerQueue.Publish(task) && w.workerQueue.IsClosed() {
						w.working.Add(-1)
						return
					}
				}
			}
			w.working.Add(-1)
		}
	}
}

func (w *Worker) Publish(task queue.Task) bool {
	select {
	case w.sem <- struct{}{}:
		go w.Run(len(w.sem))
	default:
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
}

func (w *Worker) Working() int {
	return int(w.working.Load())
}
