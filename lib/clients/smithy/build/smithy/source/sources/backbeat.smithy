namespace com.scality.backbeat

use aws.protocols#restJson1
use aws.auth#sigv4
use aws.api#service

@restJson1
@sigv4(name: "s3")
@service(sdkId: "Backbeat")
@suppress(["aws.api#ArnNamespaceMatchesServiceName"])
service Backbeat {
    version: "2017-07-01",
    operations: [
        PutData,
        GetObject,
        GetMetadata,
        PutMetadata,
        GetObjectList,
        GetBucketMetadata,
        BatchDelete,
        PutBucketIndexes,
        GetBucketIndexes,
        DeleteBucketIndexes,
        ListLifecycleCurrents,
        ListLifecycleNonCurrents,
        ListLifecycleOrphans,
        DeleteObjectFromExpiration,
        GetRaftId,
        GetRaftLog,
        GetRaftBuckets,
        GetBucketCseq,
        MultipleBackendPutObject,
        MultipleBackendHeadObject,
        MultipleBackendDeleteObject,
        MultipleBackendInitiateMPU,
        MultipleBackendPutMPUPart,
        MultipleBackendCompleteMPU,
        MultipleBackendAbortMPU,
        MultipleBackendPutObjectTagging,
        MultipleBackendDeleteObjectTagging,
    ]
}
