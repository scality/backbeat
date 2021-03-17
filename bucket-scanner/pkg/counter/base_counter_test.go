package counter_test

import (
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/scality/backbeat/bucket-scanner/pkg/counter"
)

func increment(c counter.Counter, startSignal chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	<-startSignal

	for i := 0; i < 250000; i++ {
		c.Incr(nil)
		<-time.After(time.Microsecond)
	}
}

var _ = Describe("BaseCounter", func() {
	It("should increment atomically", func() {
		wg := &sync.WaitGroup{}
		c := counter.NewBaseCounter()
		startSignal := make(chan struct{})

		wg.Add(4)
		go increment(c, startSignal, wg)
		go increment(c, startSignal, wg)
		go increment(c, startSignal, wg)
		go increment(c, startSignal, wg)

		close(startSignal)
		wg.Wait()

		Expect(c.Get()).To(Equal(int64(1000000)))
	})

	It("should implement Get", func() {
		c := counter.NewBaseCounter()

		c.Set(30)

		Expect(c.Get()).To(Equal(int64(30)))
	})

	It("should implement String", func() {
		c := counter.NewBaseCounter()

		c.Set(30)

		Expect(c.String()).To(Equal("30"))
	})
})
