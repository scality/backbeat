package replication_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/scality/backbeat/bucket-scanner/pkg/counter/replication"
	"github.com/scality/backbeat/bucket-scanner/pkg/types"
)

var _ = Describe("CounterSet", func() {
	Describe("Add", func() {
		It("should add all fields for all regions", func() {
			cs1 := replication.New("")
			cs2 := replication.New("")

			setValues(cs1.Global)
			setValues(cs1.GetTargetLocation("loc1"))
			setValues(cs2.Global)
			setValues(cs2.GetTargetLocation("loc1"))

			cs1.Add(cs2)

			expectDoubleValues(cs1.Global)
			expectDoubleValues(cs1.ByTargetLocation["loc1"])
		})
	})

	Describe("Reset", func() {
		It("should reset all values", func() {
			cs1 := replication.New("")

			setValues(cs1.Global)
			setValues(cs1.GetTargetLocation("loc1"))

			cs1.Reset()

			Expect(cs1).To(Equal(replication.New("")))
		})
	})

	Describe("CopyTo/Serialize/LoadSerialized Roundtripping", func() {
		It("should copy all values around", func() {
			cs1 := replication.New("")
			cs2 := replication.New("")
			cs3 := replication.New("")

			setValues(cs1.Global)
			setValues(cs1.GetTargetLocation("loc1"))
			cs1.CopyTo(cs2)
			s := cs2.Serialize()
			cs3.LoadSerialized(s)

			Expect(cs3).To(Equal(cs1))
		})
	})

	Describe("CountReplicationStatus", func() {
		var cs *replication.CounterSet
		obj := &types.ObjectMetadata{}

		pendingInfo := &types.ObjectReplicationInfo{
			Status: types.ReplicationStatusPending,
			Backends: []types.ReplicationBackendStatus{
				{
					Site:   "site1",
					Status: types.ReplicationStatusPending,
				},
				{
					Site:   "site2",
					Status: types.ReplicationStatusCompleted,
				},
			},
		}

		completedInfo := &types.ObjectReplicationInfo{
			Status: types.ReplicationStatusCompleted,
			Backends: []types.ReplicationBackendStatus{
				{
					Site:   "site1",
					Status: types.ReplicationStatusPending,
				},
				{
					Site:   "site2",
					Status: types.ReplicationStatusCompleted,
				},
			},
		}

		failedInfo := &types.ObjectReplicationInfo{
			Status: types.ReplicationStatusFailed,
			Backends: []types.ReplicationBackendStatus{
				{
					Site:   "site1",
					Status: types.ReplicationStatusPending,
				},
				{
					Site:   "site2",
					Status: types.ReplicationStatusFailed,
				},
			},
		}

		nostatusInfo := &types.ObjectReplicationInfo{
			Status: "",
			Backends: []types.ReplicationBackendStatus{
				{
					Site:   "site1",
					Status: types.ReplicationStatusPending,
				},
				{
					Site:   "site2",
					Status: "",
				},
			},
		}

		replicaInfo := &types.ObjectReplicationInfo{
			Status: types.ReplicationStatusReplica,
			Backends: []types.ReplicationBackendStatus{
				{
					Site:   "site1",
					Status: types.ReplicationStatusPending,
				},
				{
					Site:   "site2",
					Status: types.ReplicationStatusReplica,
				},
			},
		}

		processingInfo := &types.ObjectReplicationInfo{
			Status: types.ReplicationStatusProcessing,
			Backends: []types.ReplicationBackendStatus{
				{
					Site:   "site1",
					Status: types.ReplicationStatusPending,
				},
				{
					Site:   "site2",
					Status: types.ReplicationStatusCompleted,
				},
			},
		}

		BeforeEach(func() {
			cs = replication.New("")
		})

		It("should update global and per-location pending counters", func() {
			cs.CountReplicationStatus(pendingInfo, obj, false)

			Expect(cs.Global.Pending.Get()).To(Equal(int64(1)))
			Expect(cs.ByTargetLocation["site1"].Pending.Get()).To(BeEquivalentTo(1))
		})

		It("should update global and per-location replica counters", func() {
			cs.CountReplicationStatus(replicaInfo, obj, false)

			Expect(cs.Global.Replica.Get()).To(Equal(int64(1)))
			Expect(cs.ByTargetLocation["site2"].Replica.Get()).To(BeEquivalentTo(1))
		})

		It("should update global and per-location nostatus counters", func() {
			cs.CountReplicationStatus(nostatusInfo, obj, false)

			Expect(cs.Global.NoStatus.Get()).To(Equal(int64(1)))
			Expect(cs.ByTargetLocation["site2"].NoStatus.Get()).To(BeEquivalentTo(1))
		})

		It("should update global and per-location processing counters", func() {
			cs.CountReplicationStatus(processingInfo, obj, false)

			Expect(cs.Global.Processing.Get()).To(Equal(int64(1)))
			// PROCESSING is a synthetic status on the top-level object md, not per-backend
			// so no testing it at the backend level
			Expect(cs.ByTargetLocation["site2"].Completed.Get()).To(BeEquivalentTo(1))
		})

		It("should update global and per-location completed counters (absolute counting)", func() {
			cs.CountReplicationStatus(completedInfo, obj, false)

			Expect(cs.Global.Pending.Get()).To(BeEquivalentTo(0))
			Expect(cs.Global.Completed.Get()).To(BeEquivalentTo(1))
			Expect(cs.ByTargetLocation["site1"].Pending.Get()).To(BeEquivalentTo(1))
			Expect(cs.ByTargetLocation["site1"].Completed.Get()).To(BeEquivalentTo(0))
			Expect(cs.ByTargetLocation["site2"].Pending.Get()).To(BeEquivalentTo(0))
			Expect(cs.ByTargetLocation["site2"].Completed.Get()).To(BeEquivalentTo(1))
		})

		It("should update global and per-location completed counters (realtime transition)", func() {
			cs.CountReplicationStatus(completedInfo, obj, true)

			Expect(cs.Global.Pending.Get()).To(BeEquivalentTo(-1))
			Expect(cs.Global.Completed.Get()).To(BeEquivalentTo(1))
			Expect(cs.ByTargetLocation["site1"].Pending.Get()).To(BeEquivalentTo(1))
			Expect(cs.ByTargetLocation["site1"].Completed.Get()).To(BeEquivalentTo(0))
			Expect(cs.ByTargetLocation["site2"].Pending.Get()).To(BeEquivalentTo(-1))
			Expect(cs.ByTargetLocation["site2"].Completed.Get()).To(BeEquivalentTo(1))
		})

		It("should update global and per-location failed counters (absolute counting)", func() {
			cs.CountReplicationStatus(failedInfo, obj, false)

			Expect(cs.Global.Pending.Get()).To(BeEquivalentTo(0))
			Expect(cs.Global.Failed.Get()).To(BeEquivalentTo(1))
			Expect(cs.ByTargetLocation["site1"].Pending.Get()).To(BeEquivalentTo(1))
			Expect(cs.ByTargetLocation["site1"].Failed.Get()).To(BeEquivalentTo(0))
			Expect(cs.ByTargetLocation["site2"].Pending.Get()).To(BeEquivalentTo(0))
			Expect(cs.ByTargetLocation["site2"].Failed.Get()).To(BeEquivalentTo(1))
		})

		It("should update global and per-location failed counters (realtime transition)", func() {
			cs.CountReplicationStatus(failedInfo, obj, true)

			Expect(cs.Global.Pending.Get()).To(BeEquivalentTo(-1))
			Expect(cs.Global.Failed.Get()).To(BeEquivalentTo(1))
			Expect(cs.ByTargetLocation["site1"].Pending.Get()).To(BeEquivalentTo(1))
			Expect(cs.ByTargetLocation["site1"].Failed.Get()).To(BeEquivalentTo(0))
			Expect(cs.ByTargetLocation["site2"].Pending.Get()).To(BeEquivalentTo(-1))
			Expect(cs.ByTargetLocation["site2"].Failed.Get()).To(BeEquivalentTo(1))
		})
	})

	Describe("GetTargetLocation", func() {
		It("should return existing ByTargetLocation if available", func() {
			cs := replication.New("")
			bscs := replication.NewByStatusCounterSet("")

			cs.ByTargetLocation["site1"] = bscs

			Expect(cs.GetTargetLocation("site1")).To(BeIdenticalTo(bscs))
			Expect(cs.GetTargetLocation("site1")).To(Equal(bscs))
		})

		It("should initialize a new ByTargetLocation if needed", func() {
			cs := replication.New("cs")
			bscs := replication.NewByStatusCounterSet("cs:a")

			Expect(cs.GetTargetLocation("a")).NotTo(BeIdenticalTo(bscs))
			Expect(cs.GetTargetLocation("a")).To(Equal(bscs))
		})
	})
})
