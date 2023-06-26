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
		signal.Notify(sig, syscall.SIGUSR1)
		go func() {
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
		<-sig
		t.Log("B exit")
	}()
	id := <-tgid
	time.AfterFunc(10*time.Second, func() {
		syscall.Tgkill(id.pid, id.tid, syscall.SIGUSR1)
	})
	wg.Wait()
	t.Log("exit")
	t.Log("main is still running")
	time.Sleep(5 * time.Second)
	t.Log("main exits")
}
