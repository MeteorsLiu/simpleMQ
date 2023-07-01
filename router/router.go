package router

import (
	"math/rand"
	"sync"

	"github.com/MeteorsLiu/rand"
	"github.com/MeteorsLiu/simpleMQ/queue"
	"github.com/MeteorsLiu/simpleMQ/worker"
)

const (
	DefaultWorkerSize      = 100
	DefaultWorkerSpwanSize = 10
)

type Options func(*Router)

func WithNewQueueFunc(f queue.NewQueue) Options {
	return func(r *Router) {
		r.newQueueFunc = f
	}
}

func WithWorkerGroupCap(cap int) Options {
	return func(r *Router) {
		r.workersCap = cap
	}
}

func WithWorkerSize(size int) Options {
	return func(r *Router) {
		r.workerSize = size
	}
}

func WithWorkerSpwanSize(spwan int) Options {
	return func(r *Router) {
		r.workerSpawnSize = spwan
	}
}

type Router struct {
	sync.RWMutex
	workerSize      int
	workerSpawnSize int
	workersCap      int
	workers         []*worker.Worker
	newQueueFunc    queue.NewQueue
}

func NewRouter(opts ...Options) *Router {
	r := &Router{}
	for _, o := range opts {
		o(r)
	}
	if r.workerSize == 0 {
		r.workerSize = DefaultWorkerSize
	}

	if r.newQueueFunc == nil {
		r.newQueueFunc = queue.NewSimpleQueue
	}
	return r
}

func (r *Router) setupWorker() *worker.Worker {
	r.Lock()
	defer r.Unlock()
	if len(r.workers) >= r.workersCap && r.workersCap > 0 {
		return nil
	}
	newWorker := worker.NewWorker(r.workerSize, r.workerSpawnSize, r.newQueueFunc(), true)
	r.workers = append(r.workers, newWorker)

	return newWorker
}

func (r *Router) findWorker() *worker.Worker {
	var randomSelected *worker.Worker

	r.RLock()
	for _, w := range r.workers {
		if !w.IsBusy() {
			r.RUnlock()
			return w
		}
	}
	if len(r.workers) > 0 {
		randomSelected = r.workers[rand.Intn(len(r.workers))]
	}
	r.RUnlock()

	if tryNew := r.setupWorker(); tryNew != nil {
		return tryNew
	}

	return randomSelected
}

func (r *Router) killTask(id string) {
	r.RLock()
	defer r.RUnlock()

	for _, worker := range r.workers {
		if err := worker.KillTask(id); err == nil {
			return
		}
	}
}

func (r *Router) Dispatch(f func() error, callback ...func()) queue.Task {
	task := queue.NewTask(f)
	r.findWorker().Publish(task, callback...)
	return task
}

func (r *Router) DispatchTask(f queue.Task) {
	r.findWorker().Publish(f)
}

func (r *Router) StopByID(id string) {
	r.killTask(id)
}

func (r *Router) Stop() {
	r.RLock()
	defer r.RUnlock()
	for _, w := range r.workers {
		w.Stop()
	}
}
