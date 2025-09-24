namespace com.scality.backbeat

use aws.protocols#restJson1

/// Aborts a multipart upload for multiple backend storage
@idempotent
@http(method: "DELETE", uri: "/_/backbeat/multiplebackenddata/{Bucket}/{Key+}?operation=abortmpu")
operation MultipleBackendAbortMPU {
    input: MultipleBackendAbortMPUInput,
    output: MultipleBackendAbortMPUOutput,
}

@input
structure MultipleBackendAbortMPUInput {
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
    
    @httpHeader("X-Scal-Upload-Id")
    UploadId: String,
}

@output
structure MultipleBackendAbortMPUOutput {
}
