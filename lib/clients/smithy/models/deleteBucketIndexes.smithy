$version: "2.0"

namespace com.scality.backbeat

@http(method: "POST", uri: "/_/backbeat/index/{Bucket}?operation=delete")
operation DeleteBucketIndexes {
    input: DeleteBucketIndexesInput,
    output: DeleteBucketIndexesOutput
}

structure DeleteBucketIndexesInput {
    @required
    @httpLabel
    Bucket: String,
    
    @httpPayload
    Body: Blob
}

structure DeleteBucketIndexesOutput {
    // Empty response body
}
