package worker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/MeteorsLiu/simpleMQ/queue"
)

func TestWorker(t *testing.T) {
	q := queue.NewSimpleQueue()
	w := NewWorker(5, q)
	var cnt atomic.Int64
	var wg sync.WaitGroup
	wg.Add(5)
	w.Publish(queue.NewTask(func() error {
		defer wg.Done()
		if cnt.Add(1) != 5 {
			return fmt.Errorf("%d", cnt.Load())
		}
		return nil
	}, queue.WithNoRetryFunc()))
	wg.Wait()
}
