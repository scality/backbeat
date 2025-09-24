namespace com.scality.backbeat

use aws.protocols#restJson1

@http(method: "POST", uri: "/_/backbeat/index/{Bucket}?operation=add")
operation PutBucketIndexes {
    input: PutBucketIndexesInput,
    output: PutBucketIndexesOutput,
}

structure PutBucketIndexesInput {
    @httpLabel
    @required
    Bucket: String,
    
    @httpPayload
    Body: Blob,
}

structure PutBucketIndexesOutput {
    // Empty response structure
}
