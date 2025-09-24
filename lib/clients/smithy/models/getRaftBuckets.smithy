namespace com.scality.backbeat

use aws.protocols#restJson1

/// Retrieves buckets associated with a specific Raft log ID
@readonly
@http(method: "GET", uri: "/_/metadata/admin/raft_sessions/{LogId}/bucket")
operation GetRaftBuckets {
    input: GetRaftBucketsInput,
    output: GetRaftBucketsOutput,
}

@input
structure GetRaftBucketsInput {
    @httpLabel
    @required
    LogId: String,
}

@output
structure GetRaftBucketsOutput {
    /// List of bucket names associated with the Raft log
    Buckets: BucketNameList,
}

list BucketNameList {
    member: String,
}
