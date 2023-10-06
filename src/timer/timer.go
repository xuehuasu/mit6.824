package timer

import (
	"time"
)

type Timer struct {
	duration time.Duration
	timer    *time.Timer
	stop     chan bool
}

func NewTimer(duration time.Duration, callback func()) *Timer {
	timer := &Timer{
		duration: duration,
		timer:    time.NewTimer(duration),
		stop:     make(chan bool),
	}

	go func() { // 定时任务
		select {
		case <-timer.timer.C:
			callback()
			timer.timer.Stop()
		case <-timer.stop:
			timer.timer.Stop()
			return
		}
	}()

	// go func() { // 循环执行的定时任务
	// 	for {
	// 		select {
	// 		case <-timer.timer.C:
	// 			callback()
	// 			timer.timer.Reset(timer.duration)
	// 		case <-timer.stop:
	// 			timer.timer.Stop()
	// 			return
	// 		}
	// 	}
	// }()
	return timer
}

func (t *Timer) Reset(duration time.Duration) {
	t.duration = duration
	t.timer.Reset(duration)
}

func (t *Timer) Stop() {
	t.stop <- true
}

// func main() {
// 	fmt.Println("启动定时器")
// 	timer := NewTimer(3*time.Second, func() {
// 		fmt.Println("定时器触发")
// 	})

// 	// 模拟执行定时器任务前的其他操作
// 	time.Sleep(5 * time.Second)

// 	// 重新设置时间
// 	fmt.Println("重新设置定时器时间为5秒")
// 	timer.Reset(5 * time.Second)

// 	// 模拟执行定时器任务前的其他操作
// 	time.Sleep(7 * time.Second)

// 	// 取消定时器
// 	fmt.Println("取消定时器")
// 	timer.Stop()

// 	fmt.Println("程序结束")
// }
