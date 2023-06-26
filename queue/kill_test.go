package queue

import (
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
	wg.Add(2)
	go func() {
		defer wg.Done()
		tgid <- &PID{
			pid: syscall.Getpid(),
			tid: syscall.Gettid(),
		}

		i := 0
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				i++
				t.Log(i)
			}
		}

	}()
	go func() {
		defer wg.Done()
		id := <-tgid

		t.Log(syscall.Getpid(), syscall.Gettid(), id.pid, id.tid)
		time.AfterFunc(10*time.Second, func() {
			syscall.Tgkill(id.pid, id.tid, syscall.SIGSTOP)
		})
	}()
	wg.Wait()
	t.Log("exit")
	t.Log("main is still running")
	time.Sleep(5 * time.Second)
	t.Log("main exits")
}
