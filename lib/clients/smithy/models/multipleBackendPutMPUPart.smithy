namespace com.scality.backbeat

use aws.protocols#restJson1

/// Uploads a part for a multipart upload to multiple backend storage
@idempotent
@http(method: "PUT", uri: "/_/backbeat/multiplebackenddata/{Bucket}/{Key+}?operation=putpart")
operation MultipleBackendPutMPUPart {
    input: MultipleBackendPutMPUPartInput,
    output: MultipleBackendPutMPUPartOutput,
}

@input
structure MultipleBackendPutMPUPartInput {
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
    
    @httpHeader("X-Scal-Part-Number")
    PartNumber: Long,
    
    @httpHeader("X-Scal-Upload-Id")
    UploadId: String,
    
    @httpPayload
    Body: Blob,
}

@output
structure MultipleBackendPutMPUPartOutput {
    /// Part number
    partNumber: Long,
    
    /// ETag of the uploaded part
    ETag: String,
    
    /// Number of sub-parts
    numberSubParts: Long,
}
