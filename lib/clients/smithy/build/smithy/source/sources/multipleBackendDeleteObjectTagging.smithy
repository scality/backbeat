namespace com.scality.backbeat

use aws.protocols#restJson1

/// Removes tags from an object in multiple backend storage
@idempotent
@suppress(["HttpMethodSemantics.UnexpectedPayload"])
@http(method: "DELETE", uri: "/_/backbeat/multiplebackenddata/{Bucket}/{Key+}?operation=deleteobjecttagging")
operation MultipleBackendDeleteObjectTagging {
    input: MultipleBackendDeleteObjectTaggingInput,
    output: MultipleBackendDeleteObjectTaggingOutput,
}

@input
structure MultipleBackendDeleteObjectTaggingInput {
    @httpLabel
    @required
    Bucket: String,
    
    @httpLabel
    @required
    Key: String,
    
    @httpHeader("X-Scal-Storage-Class")
    @required
    StorageClass: String,
    
    @httpHeader("X-Scal-Storage-Type")
    StorageType: String,
    
    @httpHeader("X-Scal-Data-Store-Version-Id")
    DataStoreVersionId: String,
    
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
structure MultipleBackendDeleteObjectTaggingOutput {
    /// Version ID of the object after tag removal
    versionId: String,
}
