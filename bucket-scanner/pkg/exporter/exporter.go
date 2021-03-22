package exporter

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scality/backbeat/bucket-scanner/pkg/counter/replication"
	"github.com/scality/backbeat/bucket-scanner/pkg/types"
	log "github.com/sirupsen/logrus"
)

type (
	Exporter struct {
		Registry *prometheus.Registry
	}

	GaugeVecSet struct {
		// CompositeVersionStatusGauge is the aggregate count of objects' toplevel replication status
		CompositeVersionStatusGauge *prometheus.GaugeVec
		// StatusByDestinationRootGauge is the toplevel gauge that per-destination gauges curry from
		StatusByDestinationRootGauge *prometheus.GaugeVec
		// GaugeByDestinationMap is the drilldown of object's replication statuses per destination backend
		GaugeByDestinationMap map[types.SiteName]*prometheus.GaugeVec
	}

	GaugeVecBuilder func(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec
)

const (
	StatusPending    = "pending"
	StatusFailed     = "failed"
	StatusCompleted  = "completed"
	StatusReplica    = "replica"
	StatusNoStatus   = "nostatus"
	StatusProcessing = "processing"

	shutdownTimeout = 5 * time.Second
)

func NewExporter() *Exporter {
	return &Exporter{
		Registry: prometheus.NewRegistry(),
	}
}

func (e *Exporter) NewGaugeVecSet() *GaugeVecSet {
	var (
		compositeVersionStatusGaugeOpts = prometheus.GaugeOpts{
			Namespace: "default",
			Subsystem: "backbeat",
			Name:      "replication_versions",
			Help:      "Number of object versions with a replication policy",
		}

		compositeVersionStatusGaugeLabels = []string{
			"status",
		}

		statusByDestinationRootGaugeOpts = prometheus.GaugeOpts{
			Namespace: "default",
			Subsystem: "backbeat",
			Name:      "replication_tasks",
			Help:      "Number of replication operations of object versions to backends",
		}

		statusByDestinationRootGaugeLabels = []string{
			"status",
			"destination",
		}
	)

	return &GaugeVecSet{
		CompositeVersionStatusGauge: promauto.With(e.Registry).NewGaugeVec(
			compositeVersionStatusGaugeOpts, compositeVersionStatusGaugeLabels),
		StatusByDestinationRootGauge: promauto.With(e.Registry).NewGaugeVec(
			statusByDestinationRootGaugeOpts, statusByDestinationRootGaugeLabels),
		GaugeByDestinationMap: make(map[types.SiteName]*prometheus.GaugeVec),
	}
}

func copyCountersToGauge(v *replication.ByStatusCounterSet, g *prometheus.GaugeVec) {
	g.WithLabelValues(StatusPending).Set(float64(v.Pending.Get()))
	g.WithLabelValues(StatusFailed).Set(float64(v.Failed.Get()))
	g.WithLabelValues(StatusCompleted).Set(float64(v.Completed.Get()))
	g.WithLabelValues(StatusReplica).Set(float64(v.Replica.Get()))
	g.WithLabelValues(StatusNoStatus).Set(float64(v.NoStatus.Get()))
	g.WithLabelValues(StatusProcessing).Set(float64(v.Processing.Get()))
}

func ensureGaugeForDestination(m map[types.SiteName]*prometheus.GaugeVec,
	dest types.SiteName, parent *prometheus.GaugeVec) *prometheus.GaugeVec {
	if m[dest] == nil {
		m[dest] = parent.MustCurryWith(prometheus.Labels{"destination": string(dest)})
	}

	return m[dest]
}

func CopyCounterSetsToGauges(css []*replication.CounterSet, gvs *GaugeVecSet) {
	cs := replication.New("prom")
	for _, c := range css {
		c.Lock()
		cs.Add(c)
		c.Unlock()
	}

	copyCountersToGauge(cs.Global, gvs.CompositeVersionStatusGauge)

	for dest, v := range cs.ByTargetLocation {
		gs := ensureGaugeForDestination(gvs.GaugeByDestinationMap, dest, gvs.StatusByDestinationRootGauge)
		copyCountersToGauge(v, gs)
	}
}

func PeriodicPush(ctx context.Context, replicationCounterSets []*replication.CounterSet, gvs *GaugeVecSet, ticker <-chan time.Time, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case _, ok := <-ticker:
			if !ok {
				return
			}

			log.WithField("css", replicationCounterSets).Trace("mirroring to prom gauge")

			CopyCounterSetsToGauges(replicationCounterSets, gvs)
		}
	}
}

// Start starts an HTTP server on listenOn that exports replication metrics.
func (e *Exporter) Start(ctx context.Context, listenOn string, replicationCounterSets []*replication.CounterSet, exportInterval time.Duration, wg *sync.WaitGroup) {
	ticker := time.NewTicker(exportInterval)

	// stop metrics sync ticker on context cancelation

	wg.Add(1)

	go func() {
		defer wg.Done()

		<-ctx.Done()
		ticker.Stop()
	}()

	// serve /metrics over http

	m := http.NewServeMux()
	m.Handle("/metrics", promhttp.HandlerFor(e.Registry, promhttp.HandlerOpts{}))

	s := http.Server{Addr: listenOn, Handler: m}

	wg.Add(1)

	go func() {
		defer wg.Done()

		err := s.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	// shutdown http server on ctx cancelation

	wg.Add(1)

	go func() {
		defer wg.Done()

		<-ctx.Done()

		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		err := s.Shutdown(ctx)
		if err != nil && !errors.Is(err, ctx.Err()) {
			log.Fatal(err)
		}
	}()

	// mirror our metrics to the auto-exported prometheus metrics

	wg.Add(1)

	go PeriodicPush(ctx, replicationCounterSets, e.NewGaugeVecSet(), ticker.C, wg)
}
