package console

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gosuri/uilive"
)

var (
	_ Monitor = &InteractiveMonitor{}
	_ Channel = &InteractiveChannel{}
)

type (
	InteractiveChannel struct {
		sync.Mutex
		name   string
		value  string
		buffer bytes.Buffer
	}

	InteractiveMonitor struct {
		sync.Mutex
		channels     []*InteractiveChannel
		writer       *uilive.Writer
		echoInterval time.Duration
	}
)

func (c *InteractiveChannel) Output(msg map[string]interface{}) {
	var keys []string
	for k := range msg {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	c.Lock()
	defer c.Unlock()

	c.buffer.Reset()
	c.buffer.WriteString(c.name)
	c.buffer.WriteString(": ")

	for _, k := range keys {
		c.buffer.WriteString(k)
		c.buffer.WriteRune('=')
		c.buffer.WriteString(fmt.Sprint(msg[k]))
		c.buffer.WriteRune(' ')
	}

	c.value = c.buffer.String()
}

func (c *InteractiveChannel) getValue() string {
	c.Lock()
	defer c.Unlock()

	return c.value
}

func newInteractiveMonitor(echoInterval time.Duration) *InteractiveMonitor {
	return &InteractiveMonitor{
		writer:       uilive.New(),
		echoInterval: echoInterval,
	}
}

func (m *InteractiveMonitor) Start(ctx context.Context) {
	m.writer.Start()

	go func() {
		t := time.NewTicker(m.echoInterval)
		defer t.Stop()
		defer m.flush()

		for {
			select {
			case <-ctx.Done():
				return

			case <-t.C:
				m.flush()
			}
		}
	}()
}

func (m *InteractiveMonitor) Stop() {
	m.flush()
	m.writer.Stop()
}

func (m *InteractiveMonitor) AppendDebug(name string) Channel {
	return m.Append(name)
}

func (m *InteractiveMonitor) Append(name string) Channel {
	ch := &InteractiveChannel{
		name: name,
	}

	m.Lock()
	defer m.Unlock()

	m.channels = append(m.channels, ch)

	return ch
}

func (m *InteractiveMonitor) flush() {
	var lines []string

	m.Lock()
	for _, ch := range m.channels {
		lines = append(lines, ch.getValue())
	}
	m.Unlock()

	output := strings.Join(lines, "\n")
	fmt.Fprintln(m.writer, output)
}
