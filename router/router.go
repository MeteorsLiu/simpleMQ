package router

import (
	"sync"

	"github.com/MeteorsLiu/rand"
	"github.com/MeteorsLiu/simpleMQ/queue"
	"github.com/MeteorsLiu/simpleMQ/worker"
	"github.com/alphadose/haxmap"
)

const (
	DefaultWorkerSize      = 100
	DefaultWorkerSpwanSize = 10
	DefaultWorkerCap       = 10
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
	routerPath      *haxmap.Map[string, *worker.Worker]
}

func NewRouter(opts ...Options) *Router {
	r := &Router{
		workerSize:      DefaultWorkerSize,
		workerSpawnSize: DefaultWorkerSpwanSize,
		newQueueFunc:    queue.NewSimpleQueue,
		workersCap:      DefaultWorkerCap,
		routerPath:      haxmap.New[string, *worker.Worker](),
	}
	for _, o := range opts {
		o(r)
	}
	// new one for the task
	r.setupWorker()
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

func (r *Router) getWorkerBy(name string) *worker.Worker {
	routerWorker, ok := r.routerPath.Get(name)
	if !ok {
		routerWorker = worker.NewWorker(r.workerSize, r.workerSpawnSize, r.newQueueFunc(), true)
		r.routerPath.Set(name, routerWorker)
	}
	return routerWorker
}

func (r *Router) findWorker() *worker.Worker {
	if tryNew := r.setupWorker(); tryNew != nil {
		return tryNew
	}
	r.RLock()
	defer r.RUnlock()
	return r.workers[rand.Intn(len(r.workers))]
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

func (r *Router) Dispatch(f func() error, callback ...queue.Finalizer) queue.Task {
	task := queue.NewTask(f)
	r.findWorker().Publish(task, callback...)
	return task
}

func (r *Router) DispatchTask(f queue.Task) {
	r.findWorker().Publish(f)
}

func (r *Router) DispatchPath(path string, f func() error, callback ...queue.Finalizer) queue.Task {
	task := queue.NewTask(f)
	r.getWorkerBy(path).Publish(task, callback...)
	return task
}

func (r *Router) DispatchPathTask(path string, task queue.Task) {
	r.getWorkerBy(path).Publish(task)
}

func (r *Router) StopByID(id string) {
	r.killTask(id)
}

func (r *Router) Stop() {
	r.RLock()
	for _, w := range r.workers {
		w.Stop()
	}
	r.RUnlock()

	r.routerPath.ForEach(func(_ string, w *worker.Worker) bool {
		w.Stop()
		return true
	})
}
