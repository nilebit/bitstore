package logs

import (
	"flag"
	"log"
	"time"

	"github.com/golang/glog"
)

var logFlushFreq time.Duration
var NeverStop <-chan struct{} = make(chan struct{})

func init() {
	flag.DurationVar(&logFlushFreq, "log-flush-frequency", 5*time.Second, "Maximum number of seconds between log flushes")
}

type GlogWriter struct{}

func (writer GlogWriter) Write(data []byte) (n int, err error) {
	glog.Info(string(data))
	return len(data), nil
}

func InitLogs() {
	log.SetOutput(GlogWriter{})
	log.SetFlags(0)
	go Forever(glog.Flush, logFlushFreq)
}

func FlushLogs() {
	glog.Flush()
}

func Forever(f func(), period time.Duration) {
	Until(f, period, NeverStop)
}

func Until(f func(), period time.Duration, stopCh <-chan struct{}) {
	var t *time.Timer

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		f()
		t = newOrResetTimer(t, period)

		select {
		case <-stopCh:
			return
		case <-t.C:
		}
	}
}

func newOrResetTimer(t *time.Timer, d time.Duration) *time.Timer {
	if t == nil {
		return time.NewTimer(d)
	}
	t.Reset(d)
	return t
}
