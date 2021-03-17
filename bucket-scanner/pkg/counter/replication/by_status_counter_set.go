package replication

import (
	"github.com/scality/backbeat/bucket-scanner/pkg/counter"
	"github.com/scality/backbeat/bucket-scanner/pkg/types"
	log "github.com/sirupsen/logrus"
)

type (
	ByStatusCounterSet struct {
		Pending    counter.Counter
		Failed     counter.Counter
		Completed  counter.Counter
		Replica    counter.Counter
		NoStatus   counter.Counter
		Processing counter.Counter

		name string
	}

	SerializedByStatusCounterSet struct {
		Pending    int64 `json:"pending"`
		Failed     int64 `json:"failed"`
		Completed  int64 `json:"completed"`
		Replica    int64 `json:"replica"`
		NoStatus   int64 `json:"nostatus"`
		Processing int64 `json:"processing"`
	}
)

func NewByStatusCounterSet(name string) *ByStatusCounterSet {
	return &ByStatusCounterSet{
		Pending:    counter.NewTrackingCounter(),
		Failed:     counter.NewTrackingCounter(),
		Completed:  counter.NewTrackingCounter(),
		Replica:    counter.NewTrackingCounter(),
		NoStatus:   counter.NewTrackingCounter(),
		Processing: counter.NewTrackingCounter(),

		name: name,
	}
}

func (bscs *ByStatusCounterSet) CopyTo(other *ByStatusCounterSet) {
	other.Pending.Set(bscs.Pending.Get())
	other.Failed.Set(bscs.Failed.Get())
	other.Completed.Set(bscs.Completed.Get())
	other.Replica.Set(bscs.Replica.Get())
	other.NoStatus.Set(bscs.NoStatus.Get())
	other.Processing.Set(bscs.Processing.Get())
}

func (bscs *ByStatusCounterSet) Add(other *ByStatusCounterSet) {
	bscs.Pending.Set(bscs.Pending.Get() + other.Pending.Get())
	bscs.Failed.Set(bscs.Failed.Get() + other.Failed.Get())
	bscs.Completed.Set(bscs.Completed.Get() + other.Completed.Get())
	bscs.Replica.Set(bscs.Replica.Get() + other.Replica.Get())
	bscs.NoStatus.Set(bscs.NoStatus.Get() + other.NoStatus.Get())
	bscs.Processing.Set(bscs.Processing.Get() + other.Processing.Get())
}

func (bscs *ByStatusCounterSet) Reset() {
	bscs.Pending.Set(0)
	bscs.Failed.Set(0)
	bscs.Completed.Set(0)
	bscs.Replica.Set(0)
	bscs.NoStatus.Set(0)
	bscs.Processing.Set(0)
}

func (bscs *ByStatusCounterSet) Serialize() *SerializedByStatusCounterSet {
	return &SerializedByStatusCounterSet{
		Pending:    bscs.Pending.Get(),
		Failed:     bscs.Failed.Get(),
		Completed:  bscs.Completed.Get(),
		Replica:    bscs.Replica.Get(),
		NoStatus:   bscs.NoStatus.Get(),
		Processing: bscs.Processing.Get(),
	}
}

func (bscs *ByStatusCounterSet) LoadSerialized(s *SerializedByStatusCounterSet) {
	bscs.Pending.Set(s.Pending)
	bscs.Failed.Set(s.Failed)
	bscs.Completed.Set(s.Completed)
	bscs.Replica.Set(s.Replica)
	bscs.NoStatus.Set(s.NoStatus)
	bscs.Processing.Set(s.Processing)
}

func (bscs *ByStatusCounterSet) CountReplicationStatus(status types.ReplicationStatus, subject types.HashedNamer, isTransition bool) {
	switch status {
	case types.ReplicationStatusCompleted:
		if isTransition {
			bscs.Pending.Decr(subject)
		}
		bscs.Completed.Incr(subject)
		log.Debugf("[%s] marking completed %v isTransition=%v", bscs.name, subject, isTransition)

	case types.ReplicationStatusPending:
		bscs.Pending.Incr(subject)

	case types.ReplicationStatusFailed:
		if isTransition {
			bscs.Pending.Decr(subject)
		}
		bscs.Failed.Incr(subject)

	case types.ReplicationStatusReplica:
		bscs.Replica.Incr(subject)

	case types.ReplicationStatusProcessing:
		bscs.Processing.Incr(subject)

	default:
		bscs.NoStatus.Incr(subject)
	}
}

func (s *SerializedByStatusCounterSet) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"pending":    s.Pending,
		"failed":     s.Failed,
		"completed":  s.Completed,
		"replica":    s.Replica,
		"nostatus":   s.NoStatus,
		"processing": s.Processing,
	}
}
