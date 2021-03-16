package replication

import (
	"sync"

	"github.com/scality/backbeat/bucket-scanner/pkg/types"
)

type (
	CounterSet struct {
		sync.Mutex
		Global           *ByStatusCounterSet
		ByTargetLocation map[types.SiteName]*ByStatusCounterSet

		name string
	}

	SerializedCounterSet struct {
		Global     *SerializedByStatusCounterSet                    `json:"global"`
		ByLocation map[types.SiteName]*SerializedByStatusCounterSet `json:"byLocation"`
	}
)

func New(name string) *CounterSet {
	return &CounterSet{
		Global:           NewByStatusCounterSet(name + ":global"),
		ByTargetLocation: make(map[types.SiteName]*ByStatusCounterSet),

		name: name,
	}
}

func (cs *CounterSet) CopyTo(other *CounterSet) {
	other.Lock()
	defer other.Unlock()

	cs.Global.CopyTo(other.Global)

	other.ByTargetLocation = make(map[types.SiteName]*ByStatusCounterSet)
	for k, v := range cs.ByTargetLocation {
		n := NewByStatusCounterSet(other.name + ":" + string(k))
		v.CopyTo(n)
		other.ByTargetLocation[k] = n
	}
}

func (cs *CounterSet) Add(other *CounterSet) {
	cs.Lock()
	defer cs.Unlock()

	cs.Global.Add(other.Global)

	for k, v := range other.ByTargetLocation {
		m := cs.getTargetLocation(k)
		m.Add(v)
	}
}

func (cs *CounterSet) Reset() {
	cs.Lock()
	defer cs.Unlock()

	cs.Global.Reset()
	cs.ByTargetLocation = make(map[types.SiteName]*ByStatusCounterSet)
}

func (cs *CounterSet) GetTargetLocation(location types.SiteName) *ByStatusCounterSet {
	cs.Lock()
	defer cs.Unlock()

	return cs.getTargetLocation(location)
}

func (cs *CounterSet) getTargetLocation(location types.SiteName) *ByStatusCounterSet {
	bscs, ok := cs.ByTargetLocation[location]
	if !ok {
		bscs = NewByStatusCounterSet(cs.name + ":" + string(location))
		cs.ByTargetLocation[location] = bscs
	}

	return bscs
}

func (cs *CounterSet) Serialize() *SerializedCounterSet {
	cs.Lock()
	defer cs.Unlock()

	byLocation := map[types.SiteName]*SerializedByStatusCounterSet{}

	for location, bscs := range cs.ByTargetLocation {
		byLocation[location] = bscs.Serialize()
	}

	return &SerializedCounterSet{
		Global:     cs.Global.Serialize(),
		ByLocation: byLocation,
	}
}

func (cs *CounterSet) LoadSerialized(s *SerializedCounterSet) {
	cs.Global.LoadSerialized(s.Global)

	for k, v := range s.ByLocation {
		l := cs.GetTargetLocation(k)
		l.LoadSerialized(v)
	}
}

// CountReplicationStatus updates global counters as well as per-location breakdown
// based on the replication info. If isTransition is true, pending counters will be
// decremented to keep the total numbers balanced. Otherwise it is assumed that a new
// object is counted and the net totals are added 1.
func (cs *CounterSet) CountReplicationStatus(info *types.ObjectReplicationInfo, subject types.HashedNamer, isTransition bool) {
	cs.Global.CountReplicationStatus(info.Status, subject, isTransition)

	for _, status := range info.Backends {
		cs.GetTargetLocation(status.Site).CountReplicationStatus(status.Status, subject, isTransition)
	}
}
