package queue

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"
)

type PID struct {
	pid int
	tid int
}

func TestKill(t *testing.T) {
	var wg sync.WaitGroup
	tgid := make(chan *PID)
	wg.Add(1)
	go func() {
		defer wg.Done()
		tgid <- &PID{
			pid: syscall.Getpid(),
			tid: syscall.Gettid(),
		}

		sig := make(chan os.Signal)
		signal.Notify(sig, syscall.SIGINT)
		ticker := time.NewTicker(time.Second)
		i := 0
		for {
			select {
			case <-ticker.C:
				i++
				t.Log(i)
			case <-sig:
				t.Log("B exits")
				return
			}
		}
	}()
	id := <-tgid
	time.AfterFunc(10*time.Second, func() {
		syscall.Tgkill(id.pid, id.tid, syscall.SIGINT)
	})
	wg.Wait()
	t.Log("exit")
	t.Log("main is still running")
}
