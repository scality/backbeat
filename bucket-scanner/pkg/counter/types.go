package counter

import "github.com/scality/backbeat/bucket-scanner/pkg/types"

type Counter interface {
	Incr(subject types.HashedNamer)
	Decr(subject types.HashedNamer)
	Get() int64
	Set(i int64)
}
