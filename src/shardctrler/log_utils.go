package shardctrler

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Debugging
const debug = true

const (
	Info  string = "INFO"
	Debug string = "DEBUG"
	Warn  string = "WARN"
	Error string = "ERROR"
)

var debugStart time.Time

func init() {
	debugStart = time.Now()
	if !debug {
		logFile, err := os.Create("shardctrler.log")
		if err != nil {
			log.Fatalf("Failed to create log file: %v", err)
		}

		log.SetOutput(logFile)
	}
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func LOG_SC(topic string, format string, a ...interface{}) {
	if topic != "debug" {
		time := time.Since(debugStart).Microseconds()
		time /= 1000
		prefix := fmt.Sprintf("%06d <ShardCtrler> [%v]", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
