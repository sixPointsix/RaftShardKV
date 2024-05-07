package raft

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Debugging
const debug = false

const (
	Debug     string = "DEBUG"
	Info      string = "INFO"
	Important string = "IMPORTANT"
	Warn      string = "WARN"
	Error     string = "ERROR"
)

var debugStart time.Time

func init() {
	debugStart = time.Now()
	if !debug {
		logFile, err := os.Create("raft.log")
		if err != nil {
			log.Fatalf("Failed to create log file: %v", err)
		}

		log.SetOutput(logFile)
	}
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func LOG_RAFT(topic string, format string, a ...interface{}) {
	if topic != Debug {
		time := time.Since(debugStart).Microseconds()
		time /= 1000
		prefix := fmt.Sprintf("%06d <Raft> [%v]", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
