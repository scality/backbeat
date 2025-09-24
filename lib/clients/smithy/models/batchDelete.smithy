$version: "2.0"

namespace com.scality.backbeat

@idempotent
@http(method: "POST", uri: "/_/backbeat/batchdelete/{Bucket}/{Key}")
operation BatchDelete {
    input: BatchDeleteInput,
    output: BatchDeleteOutput
}

structure BatchDeleteInput {
    @required
    @httpLabel
    Bucket: String,
    @required
    @httpLabel
    Key: String,
    @httpHeader("If-Unmodified-Since")
    IfUnmodifiedSince: String,
    @httpHeader("X-Scal-Storage-Class")
    StorageClass: String,
    @httpHeader("X-Scal-Tags")
    Tags: String,
    @httpHeader("X-Scal-Content-Type")
    ContentType: String,
    /// List of locations to delete
    Locations: BatchDeleteLocationList
}

list BatchDeleteLocationList {
    member: BatchDeleteLocation
}

structure BatchDeleteLocation {
    /// The data store name where the object is stored
    @required
    dataStoreName: String,
    /// The storage key for the object
    @required
    key: String,
    /// Size of the object in bytes
    size: Integer,
    /// Version ID in the data store
    dataStoreVersionId: String
}

structure BatchDeleteOutput {
    // Empty structure as per API specification
}
