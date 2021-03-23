package console

import "context"

type (
	Channel interface {
		Output(msg map[string]interface{})
	}

	Monitor interface {
		Start(ctx context.Context)
		Stop()

		Append(name string) Channel
		AppendDebug(name string) Channel
	}
)
