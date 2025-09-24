$version: "2.0"

namespace com.scality.backbeat

@idempotent
@http(method: "PUT", uri: "/_/backbeat/multiplebackenddata/{Bucket}/{Key}?operation=putobject")
operation MultipleBackendPutObject {
    input: MultipleBackendPutObjectInput,
    output: MultipleBackendPutObjectOutput
}

structure MultipleBackendPutObjectInput {
    @required
    @httpLabel
    Bucket: String,
    @required
    @httpLabel
    Key: String,
    @httpHeader("Content-MD5")
    ContentMD5: String,
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
    @httpHeader("X-Scal-Canonical-Id")
    CanonicalID: String,
    @required
    @httpHeader("X-Scal-Storage-Class")
    StorageClass: String,
    @httpHeader("X-Scal-Storage-Type")
    StorageType: String,
    @httpHeader("X-Scal-Version-Id")
    VersionId: String,
    @httpHeader("X-Scal-Tags")
    Tags: String,
    @httpPayload
    Body: Blob
}

structure MultipleBackendPutObjectOutput {
    /// Version ID of the stored object
    versionId: String,
    /// List of storage locations where the object was stored
    location: LocationMDList
}

list LocationMDList {
    member: LocationMDObj
}

structure LocationMDObj {
    /// Storage key for this location
    key: String,
    /// Size of the data stored at this location
    size: Integer,
    /// Start position/offset for this data segment
    start: Integer,
    /// Name of the data store where this is located
    dataStoreName: String,
    /// Type of the data store (e.g., file, mem, etc.)
    dataStoreType: String,
    /// ETag from the data store for this location
    dataStoreETag: String,
    /// Version ID in the data store for this location
    dataStoreVersionId: String
}
