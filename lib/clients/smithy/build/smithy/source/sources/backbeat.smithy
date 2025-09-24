namespace com.scality.backbeat

use aws.protocols#restJson1
use aws.auth#sigv4
use aws.api#service

@restJson1
@sigv4(name: "s3")
@service(sdkId: "Backbeat")
service Backbeat {
    version: "2017-07-01",
    operations: [
        PutData,
        GetObject,
        GetMetadata,
        PutMetadata,
        GetObjectList,
        PutBucketIndexes,
        GetBucketIndexes,
        DeleteBucketIndexes,
        ListLifecycleCurrents,
        ListLifecycleNonCurrents,
        ListLifecycleOrphans,
        DeleteObjectFromExpiration,
        GetBucketMetadata,
        BatchDelete,
        MultipleBackendPutObject,
        MultipleBackendDeleteObject,
        GetRaftId,
        GetRaftLog,
        GetRaftBuckets,
        GetBucketCseq,
        MultipleBackendHeadObject,
        MultipleBackendPutMPUPart,
        MultipleBackendInitiateMPU,
        MultipleBackendAbortMPU,
        MultipleBackendCompleteMPU,
        MultipleBackendPutObjectTagging,
        MultipleBackendDeleteObjectTagging,
    ]
}
