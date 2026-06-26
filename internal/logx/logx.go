package logx

import (
	"fmt"
	"log"
	"time"
)

// Info prints a timestamped component-prefixed log line.
func Info(component, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("[%s] [%s] %s", time.Now().Format("2006-01-02 15:04:05.000"), component, msg)
}
