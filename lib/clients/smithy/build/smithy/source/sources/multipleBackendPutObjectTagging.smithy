namespace com.scality.backbeat

use aws.protocols#restJson1

/// Adds or updates tags for an object in multiple backend storage
@http(method: "POST", uri: "/_/backbeat/multiplebackenddata/{Bucket}/{Key+}?operation=puttagging")
operation MultipleBackendPutObjectTagging {
    input: MultipleBackendPutObjectTaggingInput,
    output: MultipleBackendPutObjectTaggingOutput,
}

@input
structure MultipleBackendPutObjectTaggingInput {
    @httpLabel
    @required
    Bucket: String,
    
    @httpLabel
    @required
    Key: String,
    
    @httpHeader("X-Scal-Storage-Type")
    StorageType: String,
    
    @httpHeader("X-Scal-Storage-Class")
    @required
    StorageClass: String,
    
    @httpHeader("X-Scal-Data-Store-Version-Id")
    DataStoreVersionId: String,
    
    @httpHeader("X-Scal-Tags")
    Tags: String,
    
    @httpHeader("X-Scal-Source-Bucket")
    SourceBucket: String,
    
    @httpHeader("X-Scal-Source-Version-Id")
    SourceVersionId: String,
    
    @httpHeader("X-Scal-Replication-Endpoint-Site")
    ReplicationEndpointSite: String,
    
    @httpPayload
    Body: Blob,
}

@output
structure MultipleBackendPutObjectTaggingOutput {
    /// Version ID of the tagged object
    versionId: String,
}
