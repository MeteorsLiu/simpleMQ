package worker

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MeteorsLiu/simpleMQ/queue"
)

func TestWorker(t *testing.T) {
	q := queue.NewSimpleQueue()
	w := NewWorker(5, 5, q)
	var cnt atomic.Int64
	var wg sync.WaitGroup
	wg.Add(6)
	w.Publish(queue.NewTask(func() error {
		defer wg.Done()
		c := cnt.Add(1) - 1
		if c != 5 {
			return fmt.Errorf("%d", c)
		}
		t.Log("success")
		return nil
	}, queue.WithNoRetryFunc()))
	wg.Wait()
	t.Log("start to execute the retry task")
	cnt.Store(0)
	var wg1 sync.WaitGroup
	t1 := time.Now()
	wg1.Add(6)
	w.Publish(queue.NewTask(func() error {
		defer wg1.Done()
		c := cnt.Add(1) - 1
		if c != 5 {
			return fmt.Errorf("%d", c)
		}
		// 1+2+4+8+16 = 31s
		t.Log("retry success", time.Since(t1))
		return nil
	}))
	wg1.Wait()
	t1 = time.Now()
	task := queue.NewTask(func() error {
		c := cnt.Add(1) - 1
		if c != 5 {
			return fmt.Errorf("%d", c)
		}
		// 1+2+4+8+16 = 31s
		t.Log("retry success", time.Since(t1))
		return nil
	})
	go func() {
		time.AfterFunc(5*time.Second, func() {
			task.Stop()
		})
	}()
	w.Publish(task)
	time.Sleep(10 * time.Second)
}

func TestUnlimited(t *testing.T) {
	w := NewWorker(0, 0, nil, true)
	t.Log(w.unlimited)
	var cnt atomic.Int64
	t1 := time.Now()
	task := queue.NewTask(func() error {
		c := cnt.Add(1) - 1
		if c != 5 {
			return fmt.Errorf("%d", c)
		}
		return nil
	})
	w.Publish(task, func(ok bool, task queue.Task) {
		if ok {
			// 1+2+4+8+16 = 31s
			t.Log("retry success", time.Since(t1))
		} else {
			// 1+2+4+8+16 = 31s
			t.Log("retry fail", time.Since(t1))
		}

	})
	w.Wait()
	t1 = time.Now()
	for i := 0; i < 2000; i++ {
		w.Publish(queue.NewTask(func() error {
			time.Sleep(10 * time.Second)
			return nil
		}), func(ok bool, task queue.Task) {
			if ok {
				// 1+2+4+8+16 = 31s
				t.Log("retry success", time.Since(t1))
			} else {
				// 1+2+4+8+16 = 31s
				t.Log("retry fail", time.Since(t1))
			}

		})
	}
	w.Wait(5 * time.Second)
}

func TestWorkerOnce(t *testing.T) {
	w := NewWorker(0, 0, nil)
	neverStop := queue.NewTask(func() error {
		t.Log("run once")
		return errors.New("nerver stop")
	}, queue.WithNoRetryFunc())

	w.Publish(neverStop)

	neverStop.Wait()
}

func TestWorkerSync(t *testing.T) {
	w := NewWorker(0, 0, nil)
	task := queue.NewTask(func() error {
		t.Log("run once")
		return nil
	})

	w.PublishSync(task)

	t.Log(task.IsDone())
}
