package queue

import (
	"sync"
	"syscall"
	"testing"
	"time"
	_ "unsafe"

	"github.com/MeteorsLiu/getm"
)

type PID struct {
	pid int
	tid int
	m   uintptr
}

//go:linkname preemptM runtime.preemptM
func preemptM(mp uintptr)
func TestKill(t *testing.T) {
	var wg sync.WaitGroup
	tgid := make(chan *PID)
	wg.Add(2)
	go func() {
		defer wg.Done()
		tgid <- &PID{
			pid: syscall.Getpid(),
			tid: syscall.Gettid(),
			m:   getm.GetM(),
		}

		for {
			i := 0
			i++
		}

	}()
	go func() {
		defer wg.Done()
		id := <-tgid

		t.Log(syscall.Getpid(), syscall.Gettid(), id.pid, id.tid)
		time.AfterFunc(10*time.Second, func() {
			preemptM(id.m)
		})
	}()
	wg.Wait()
	t.Log("exit")
	t.Log("main is still running")
	time.Sleep(5 * time.Second)
	t.Log("main exits")
}
