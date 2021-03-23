package console

import (
	"time"
)

func NewMonitor(echoInterval time.Duration, interactive bool) Monitor {
	if interactive {
		return newInteractiveMonitor(echoInterval)
	}

	return &LoggingMonitor{}
}
