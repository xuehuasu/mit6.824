package raft

import (
	"log"
)

// Debugging
const Debug = true

func DPrintf(condition bool, format string, a ...interface{}) (n int, err error) {
	if Debug && condition {
		log.Printf(format, a...)
	}
	return
}
