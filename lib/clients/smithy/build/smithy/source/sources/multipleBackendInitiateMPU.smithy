namespace com.scality.backbeat

use aws.protocols#restJson1

/// Initiates a multipart upload for multiple backend storage
@http(method: "POST", uri: "/_/backbeat/multiplebackenddata/{Bucket}/{Key+}?operation=initiatempu")
operation MultipleBackendInitiateMPU {
    input: MultipleBackendInitiateMPUInput,
    output: MultipleBackendInitiateMPUOutput,
}

@input
structure MultipleBackendInitiateMPUInput {
    @httpLabel
    @required
    Bucket: String,
    
    @httpLabel
    @required
    Key: String,
    
    @httpHeader("X-Scal-Storage-Class")
    @required
    StorageClass: String,
    
    @httpHeader("X-Scal-Version-Id")
    VersionId: String,
    
    @httpHeader("X-Scal-Storage-Type")
    StorageType: String,
    
    @httpHeader("X-Scal-Content-Type")
    ContentType: String,
    
    @httpHeader("X-Scal-User-Metadata")
    UserMetaData: String,
    
    @httpHeader("X-Scal-Cache-Control")
    CacheControl: String,
    
    @httpHeader("X-Scal-Content-Disposition")
    ContentDisposition: String,
    
    @httpHeader("X-Scal-Content-Encoding")
    ContentEncoding: String,
    
    @httpHeader("X-Scal-Tags")
    Tags: String,
    
    @httpPayload
    Body: Blob,
}

@output
structure MultipleBackendInitiateMPUOutput {
    /// Upload ID for the multipart upload
    uploadId: String,
}
