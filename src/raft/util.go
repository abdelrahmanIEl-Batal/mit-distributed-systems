package raft

import "log"

// Debugging
const DEBUG_PRINT = false

func DebugPrint(format string, a ...interface{}) (n int, err error) {
	if DEBUG_PRINT {
		log.Printf(format, a...)
	}
	return
}
