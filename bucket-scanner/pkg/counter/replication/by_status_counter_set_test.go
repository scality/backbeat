package replication_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/scality/backbeat/bucket-scanner/pkg/counter/replication"
	"github.com/scality/backbeat/bucket-scanner/pkg/types"
)

func setValues(cs *replication.ByStatusCounterSet) {
	cs.Completed.Set(1)
	cs.Failed.Set(2)
	cs.NoStatus.Set(3)
	cs.Pending.Set(4)
	cs.Replica.Set(5)
	cs.Processing.Set(6)
}

func expectDoubleValues(cs *replication.ByStatusCounterSet) {
	Expect(cs.Serialize()).To(Equal(&replication.SerializedByStatusCounterSet{
		Pending:    int64(8),
		Failed:     int64(4),
		Completed:  int64(2),
		Replica:    int64(10),
		NoStatus:   int64(6),
		Processing: int64(12),
	}))
}

var _ = Describe("ByStatusCounterSet", func() {
	Describe("Reset", func() {
		It("should reset all values", func() {
			cs1 := replication.NewByStatusCounterSet("")
			cs2 := replication.NewByStatusCounterSet("")

			setValues(cs1)
			cs1.Reset()

			Expect(cs1).To(Equal(cs2))
		})
	})

	Describe("Add", func() {
		It("should add all fields", func() {
			cs1 := replication.NewByStatusCounterSet("")
			cs2 := replication.NewByStatusCounterSet("")

			setValues(cs1)
			setValues(cs2)

			cs1.Add(cs2)

			expectDoubleValues(cs1)
		})
	})

	Describe("toMap", func() {
		It("should copy all values around", func() {
			cs := replication.NewByStatusCounterSet("")
			setValues(cs)
			Expect(cs.Serialize().ToMap()).To(Equal(map[string]interface{}{
				"completed":  int64(1),
				"failed":     int64(2),
				"nostatus":   int64(3),
				"pending":    int64(4),
				"replica":    int64(5),
				"processing": int64(6),
			}))
		})
	})

	Describe("CopyTo/Serialize/LoadSerialized Roundtripping", func() {
		It("should copy all values around", func() {
			cs1 := replication.NewByStatusCounterSet("")
			cs2 := replication.NewByStatusCounterSet("")
			cs3 := replication.NewByStatusCounterSet("")

			setValues(cs1)
			cs1.CopyTo(cs2)
			s := cs2.Serialize()
			cs3.LoadSerialized(s)

			Expect(cs3).To(Equal(cs1))
		})
	})

	Describe("CountReplicationStatus", func() {
		obj := &types.ObjectMetadata{}
		var cs *replication.ByStatusCounterSet

		BeforeEach(func() {
			cs = replication.NewByStatusCounterSet("")
		})

		It("should count pending objects", func() {
			cs.CountReplicationStatus(types.ReplicationStatusPending, obj, false)
			Expect(cs.Pending.Get()).To(BeEquivalentTo(1))
		})

		It("should count failed objects (absolute counting)", func() {
			cs.CountReplicationStatus(types.ReplicationStatusFailed, obj, false)
			Expect(cs.Failed.Get()).To(BeEquivalentTo(1))
		})

		It("should count completed objects (absolute counting)", func() {
			cs.CountReplicationStatus(types.ReplicationStatusCompleted, obj, false)
			Expect(cs.Completed.Get()).To(BeEquivalentTo(1))
		})

		It("should count failed objects (realtime transition)", func() {
			cs.CountReplicationStatus(types.ReplicationStatusFailed, obj, true)
			Expect(cs.Failed.Get()).To(BeEquivalentTo(1))
			Expect(cs.Pending.Get()).To(BeEquivalentTo(-1))
		})

		It("should count completed objects (realtime transition)", func() {
			cs.CountReplicationStatus(types.ReplicationStatusCompleted, obj, true)
			Expect(cs.Completed.Get()).To(BeEquivalentTo(1))
			Expect(cs.Pending.Get()).To(Equal(int64(-1)))
		})

		It("should count replica objects", func() {
			cs.CountReplicationStatus(types.ReplicationStatusReplica, obj, false)
			Expect(cs.Replica.Get()).To(BeEquivalentTo(1))
		})

		It("should count nostatus objects", func() {
			cs.CountReplicationStatus("", obj, false)
			Expect(cs.NoStatus.Get()).To(BeEquivalentTo(1))
		})

		It("should count processing objects", func() {
			cs.CountReplicationStatus(types.ReplicationStatusProcessing, obj, false)
			Expect(cs.Processing.Get()).To(BeEquivalentTo(1))
		})
	})
})
