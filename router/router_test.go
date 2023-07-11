package router

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/MeteorsLiu/simpleMQ/queue"
)

func TestRouter(t *testing.T) {
	r := NewRouter()

	for i := 0; i < 5; i++ {
		i := i
		r.DispatchTask(queue.NewTask(func() error {
			t.Log(i)
			return fmt.Errorf("test" + strconv.Itoa(i))
		}, queue.WithNoRetryFunc()))
	}
	time.Sleep(10 * time.Second)
}

func TestRouterPath(t *testing.T) {
	r := NewRouter()

	for i := 0; i < 5; i++ {
		i := i
		r.DispatchPathTask("/red", queue.NewTask(func() error {
			t.Log(i)
			return fmt.Errorf("test-red" + strconv.Itoa(i))
		}, queue.WithNoRetryFunc()))
	}
	time.Sleep(2 * time.Minute)
}
