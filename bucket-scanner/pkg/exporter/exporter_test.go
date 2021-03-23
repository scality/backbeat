package exporter_test

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-resty/resty/v2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/scality/backbeat/bucket-scanner/pkg/counter/replication"
	"github.com/scality/backbeat/bucket-scanner/pkg/exporter"
)

func setValues(parent *replication.CounterSet, bscs *replication.ByStatusCounterSet) {
	parent.Lock()
	defer parent.Unlock()

	bscs.Completed.Set(1)
	bscs.Failed.Set(2)
	bscs.NoStatus.Set(3)
	bscs.Pending.Set(4)
	bscs.Replica.Set(5)
	bscs.Processing.Set(6)
}

func storeMetricByLabel(d *dto.Metric, statuses map[string]interface{}, expectDestination ...string) {
	for _, v := range d.GetLabel() {
		if v.GetName() == "status" {
			statuses[v.GetValue()] = int64(d.Gauge.GetValue())
		} else {
			Expect(v.GetName()).To(Equal("destination"))
			Expect(expectDestination).To(ContainElement(v.GetValue()))
		}
	}
}

func collectStatuses(g *prometheus.GaugeVec, expectedMetrics int, expectDestination ...string) map[string]interface{} {
	ch := make(chan prometheus.Metric, expectedMetrics)
	statuses := map[string]interface{}{}
	nMetrics := 0

	g.Collect(ch)

	for nMetrics < expectedMetrics {
		m := <-ch
		d := &dto.Metric{}

		err := m.Write(d)
		Expect(err).NotTo(HaveOccurred())

		storeMetricByLabel(d, statuses, expectDestination...)

		nMetrics++
	}

	return statuses
}

var _ = Describe("Prometheus Exporter", func() {
	interval := 200 * time.Millisecond
	listenOn := "localhost:14444"

	var e *exporter.Exporter
	var wg *sync.WaitGroup
	var cs1, cs2, csExpectedSum *replication.CounterSet

	BeforeEach(func() {
		e = exporter.NewExporter()
		wg = &sync.WaitGroup{}
		cs1 = replication.New("")
		cs2 = replication.New("")
		csExpectedSum = replication.New("")
	})

	Describe("CopyCounterSetsToGauges", func() {
		It("should aggregate input CounterSets (global version status)", func(done Done) {
			defer close(done)

			expectMetrics := 6 // 6 possible statuses, all aggregated

			setValues(cs1, cs1.Global)
			setValues(cs2, cs2.Global)

			csExpectedSum.Add(cs1)
			csExpectedSum.Add(cs2)

			gvs := e.NewGaugeVecSet()

			exporter.CopyCounterSetsToGauges([]*replication.CounterSet{cs1, cs2}, gvs)

			Expect(collectStatuses(gvs.CompositeVersionStatusGauge, expectMetrics)).
				To(Equal(csExpectedSum.Global.Serialize().ToMap()))
		})

		It("should aggregate input CounterSets (per-backend version status)", func(done Done) {
			defer close(done)

			expectMetrics := 6 // 6 possible statuses, not aggregated but only 1 distinct destination

			setValues(cs1, cs1.GetTargetLocation("loc1"))
			setValues(cs2, cs2.GetTargetLocation("loc1"))

			csExpectedSum.Add(cs1)
			csExpectedSum.Add(cs2)

			gvs := e.NewGaugeVecSet()

			exporter.CopyCounterSetsToGauges([]*replication.CounterSet{cs1, cs2}, gvs)

			Expect(collectStatuses(gvs.GaugeByDestinationMap["loc1"], expectMetrics, "loc1")).
				To(Equal(csExpectedSum.ByTargetLocation["loc1"].Serialize().ToMap()))
		})

		It("should not leak between per-backend counts", func(done Done) {
			defer close(done)

			expectMetrics := 12 // 6 possible statuses times 2 destinations

			setValues(cs1, cs1.GetTargetLocation("loc1"))
			setValues(cs2, cs2.GetTargetLocation("loc2"))

			gvs := e.NewGaugeVecSet()

			exporter.CopyCounterSetsToGauges([]*replication.CounterSet{cs1, cs2}, gvs)

			Expect(collectStatuses(gvs.GaugeByDestinationMap["loc1"], expectMetrics, "loc1", "loc2")).
				To(Equal(cs1.ByTargetLocation["loc1"].Serialize().ToMap()))
		})
	})

	Describe("PeriodicPush", func() {
		It("should update promauto gauges at given interval", func(done Done) {
			defer close(done)

			expectMetrics := 6 // 6 possible statuses, not aggregated but only 1 distinct destination
			ticker := make(chan time.Time)
			ctx := context.TODO()
			gvs := e.NewGaugeVecSet()

			wg.Add(1)
			go exporter.PeriodicPush(ctx, []*replication.CounterSet{cs1, cs2}, gvs, ticker, wg)

			tick := func(cs *replication.CounterSet) {
				setValues(cs, cs.Global)
				csExpectedSum.Add(cs)
				ticker <- time.Now()
				<-time.After(interval)

				Expect(collectStatuses(gvs.CompositeVersionStatusGauge, expectMetrics)).
					To(Equal(csExpectedSum.Global.Serialize().ToMap()))
			}

			tick(cs1)
			tick(cs2)

			close(ticker)
			wg.Wait()
		})

		It("should quit on context cancelation", func(done Done) {
			defer close(done)

			ticker := make(chan time.Time)
			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()

			wg.Add(1)
			go exporter.PeriodicPush(ctx, []*replication.CounterSet{}, nil, ticker, wg)

			cancel()
			wg.Wait()
		})

		It("should quit on ticker close", func(done Done) {
			defer close(done)

			ticker := make(chan time.Time)
			ctx := context.TODO()

			wg.Add(1)
			go exporter.PeriodicPush(ctx, []*replication.CounterSet{}, nil, ticker, wg)

			close(ticker)
			wg.Wait()
		})
	})

	Describe("StartPrometheusExporter", func() {
		waitAndGather := func() map[string]interface{} {
			<-time.After(2 * interval)

			metrics, err := e.Registry.Gather()
			Expect(err).NotTo(HaveOccurred())

			var metric *dto.MetricFamily

			for _, m := range metrics {
				if m.GetName() == "default_backbeat_replication_versions" {
					metric = m
				}
			}

			Expect(metric).NotTo(BeNil())

			statuses := make(map[string]interface{})

			for _, m := range metric.GetMetric() {
				storeMetricByLabel(m, statuses)
			}

			return statuses
		}

		It("should instanciate self-exporting metrics for promauto", func(done Done) {
			defer close(done)

			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()

			e.Start(ctx, listenOn, []*replication.CounterSet{cs1}, interval, wg)
			<-time.After(2 * interval)

			statuses := waitAndGather()

			for k := range cs1.Global.Serialize().ToMap() {
				Expect(statuses).To(HaveKey(k))
			}
		})

		It("should start periodic promauto sync", func(done Done) {
			defer close(done)

			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()

			e.Start(ctx, listenOn, []*replication.CounterSet{cs1}, interval, wg)

			setValues(cs1, cs1.Global)

			statuses := waitAndGather()
			Expect(statuses).To(Equal(cs1.Global.Serialize().ToMap()))
		})

		It("should start http server", func(done Done) {
			defer close(done)

			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()

			e.Start(ctx, listenOn, []*replication.CounterSet{cs1, cs2}, interval, wg)

			setValues(cs1, cs1.Global)
			setValues(cs1, cs1.GetTargetLocation("us-west-2"))
			setValues(cs2, cs2.Global)
			setValues(cs2, cs2.GetTargetLocation("us-west-1"))
			<-time.After(2 * interval)

			resp, err := resty.New().R().Get("http://" + listenOn + "/metrics")

			Expect(err).NotTo(HaveOccurred())
			Expect(resp.RawResponse).To(HaveHTTPStatus(http.StatusOK))

			body := string(resp.Body())
			expectExported := `
				# HELP default_backbeat_replication_tasks Number of replication operations of object versions to backends
				# TYPE default_backbeat_replication_tasks gauge
				default_backbeat_replication_tasks{destination="us-west-1",status="completed"} 1
				default_backbeat_replication_tasks{destination="us-west-1",status="failed"} 2
				default_backbeat_replication_tasks{destination="us-west-1",status="nostatus"} 3
				default_backbeat_replication_tasks{destination="us-west-1",status="pending"} 4
				default_backbeat_replication_tasks{destination="us-west-1",status="processing"} 6
				default_backbeat_replication_tasks{destination="us-west-1",status="replica"} 5
				default_backbeat_replication_tasks{destination="us-west-2",status="completed"} 1
				default_backbeat_replication_tasks{destination="us-west-2",status="failed"} 2
				default_backbeat_replication_tasks{destination="us-west-2",status="nostatus"} 3
				default_backbeat_replication_tasks{destination="us-west-2",status="pending"} 4
				default_backbeat_replication_tasks{destination="us-west-2",status="processing"} 6
				default_backbeat_replication_tasks{destination="us-west-2",status="replica"} 5
				# HELP default_backbeat_replication_versions Number of object versions
				# TYPE default_backbeat_replication_versions gauge
				default_backbeat_replication_versions{status="completed"} 2
				default_backbeat_replication_versions{status="failed"} 4
				default_backbeat_replication_versions{status="nostatus"} 6
				default_backbeat_replication_versions{status="pending"} 8
				default_backbeat_replication_versions{status="processing"} 12
				default_backbeat_replication_versions{status="replica"} 10
			`

			for _, s := range strings.Split(expectExported, "\n") {
				Expect(body).To(ContainSubstring(strings.TrimSpace(s)))
			}
		})

		It("should stop http server on context cancelation", func(done Done) {
			defer close(done)

			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()

			e.Start(ctx, listenOn, []*replication.CounterSet{}, interval, wg)

			<-time.After(2 * interval)
			cancel()

			wg.Wait()

			_, err := resty.New().R().Get("http://" + listenOn + "/metrics")
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(syscall.ECONNREFUSED))
		})
	})
})
