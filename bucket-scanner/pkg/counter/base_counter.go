package counter

import (
	"fmt"
	"sync/atomic"

	"github.com/scality/backbeat/bucket-scanner/pkg/types"
)

type baseCounter struct {
	count int64
}

// NewBaseCounter returns an atomic counter
func NewBaseCounter() *baseCounter {
	return &baseCounter{}
}

func (c *baseCounter) Incr(key types.HashedNamer) {
	atomic.AddInt64(&c.count, 1)
}

func (c *baseCounter) Decr(key types.HashedNamer) {
	atomic.AddInt64(&c.count, -1)
}

func (c *baseCounter) Get() int64 {
	return atomic.LoadInt64(&c.count)
}

func (c *baseCounter) Set(i int64) {
	atomic.StoreInt64(&c.count, i)
}

func (c *baseCounter) String() string {
	return fmt.Sprint(c.Get())
}
