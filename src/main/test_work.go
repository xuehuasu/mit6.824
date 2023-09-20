package main

import (
	"fmt"
	"time"
)

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

var achievemap chan bool

func Achievemap() {
	for {
		fmt.Print("a\n")
		achievemap <- true
		time.Sleep(1)
	}
}

func main() {
	go Achievemap()
	time.Sleep(3)
}
