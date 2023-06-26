package queue

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestKill(t *testing.T) {
	var wg sync.WaitGroup
	tgid := make(chan int)
	pid := syscall.Getpid()
	wg.Add(1)
	go func() {
		defer wg.Done()
		tgid <- syscall.Gettid()

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
				return
			}
		}
	}()
	id := <-tgid
	time.AfterFunc(10*time.Second, func() {
		syscall.Tgkill(pid, id, syscall.SIGSTOP)
	})
	wg.Wait()
	t.Log("exit")
}
