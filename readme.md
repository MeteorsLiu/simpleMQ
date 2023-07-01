# Simple Message Queue

一个简单的，开箱即用的消息队列

A simple and ready-to-use message queue for golang.

Example(例子)：

```
import (
    "github.com/MeteorsLiu/simpleMQ/router"
)

...

r := router.NewRouter()
// dispath a task
task := r.Dispath(func () error {
    time.Sleep(5*time.Second)
    fmt.Println("Hello")
    return nil
})

// stop the task, but it will still run.
task.Stop()

```


If you need to wait the result

```
realTask := func () (string, error) {
    time.Sleep(5*time.Second)
    return fmt.Sprintf("Hello"), nil
}

callback := make(chan string)
// dispath a task
task := r.Dispath(func () error {
    ret, err := realTask()
    if err == nil {
         callback <- ret
    }
    return err
})

result := <-callback
```