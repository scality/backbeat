package console

import (
	"context"

	log "github.com/sirupsen/logrus"
)

var (
	_ Channel = &LoggingChannel{}
	_ Monitor = &LoggingMonitor{}
)

type (
	LoggingChannel struct {
		log     *log.Entry
		isDebug bool
	}

	LoggingMonitor struct {
	}
)

func (c *LoggingChannel) Output(msg map[string]interface{}) {
	l := c.log.WithFields(msg)

	if c.isDebug {
		l.Debug("debug")
	} else {
		l.Info("progress report")
	}
}

func (m *LoggingMonitor) Start(ctx context.Context) {
}

func (m *LoggingMonitor) Stop() {
}

func (m *LoggingMonitor) Append(name string) Channel {
	return &LoggingChannel{
		log: log.WithField("name", name),
	}
}

func (m *LoggingMonitor) AppendDebug(name string) Channel {
	return &LoggingChannel{
		log:     log.WithField("name", name),
		isDebug: true,
	}
}
