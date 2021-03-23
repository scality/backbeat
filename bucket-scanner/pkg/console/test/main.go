package main

import (
	"context"
	"time"

	"github.com/scality/backbeat/bucket-scanner/pkg/console"
)

const delay = 200 * time.Millisecond

func testMonitor(ctx context.Context, c console.Monitor) {
	ch1 := c.Append("component1")
	ch2 := c.Append("component2")
	ch3 := c.Append("component3")

	c.Start(ctx)

	for i := 0; i < 10; i++ {
		ch1.Output(map[string]interface{}{
			"a": i,
		})

		<-time.After(delay)

		ch2.Output(map[string]interface{}{
			"c": i + 1,
		})

		<-time.After(delay)

		ch3.Output(map[string]interface{}{
			"e": i - 1,
		})

		<-time.After(delay)
	}

	c.Stop()
}

func main() {
	ctx := context.Background()

	testMonitor(ctx, console.NewMonitor(delay, true))
	testMonitor(ctx, console.NewMonitor(delay, false))
}
