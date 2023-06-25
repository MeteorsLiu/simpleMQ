package worker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MeteorsLiu/simpleMQ/queue"
)

func TestWorker(t *testing.T) {
	q := queue.NewSimpleQueue()
	w := NewWorker(5, q)
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
