package shardkv

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Debugging
const debug = true

const (
	Client   string = "CLIENT"
	Server   string = "SEVER"
	Debug    string = "Debug"
	Info     string = "INFO"
	Impotant string = "IMPORTANT"
	Warn     string = "WARN"
	Error    string = "ERROR"
)

var debugStart time.Time

func init() {
	debugStart = time.Now()
	if !debug {
		logFile, err := os.Create("shardkv.log")
		if err != nil {
			log.Fatalf("Failed to create log file: %v", err)
		}

		log.SetOutput(logFile)
	}
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func LOG(topic string, format string, a ...interface{}) {
	if topic != "debug" {
		time := time.Since(debugStart).Microseconds()
		time /= 1000
		prefix := fmt.Sprintf("%06d [%v]", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
