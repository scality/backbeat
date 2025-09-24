namespace com.scality.backbeat

use aws.protocols#restJson1

/// Retrieves metadata for an object from multiple backend storage
@readonly
@http(method: "GET", uri: "/_/backbeat/multiplebackendmetadata/{Bucket}/{Key+}")
operation MultipleBackendHeadObject {
    input: MultipleBackendHeadObjectInput,
    output: MultipleBackendHeadObjectOutput,
}

@input
structure MultipleBackendHeadObjectInput {
    @httpLabel
    @required
    Bucket: String,
    
    @httpLabel
    @required
    Key: String,
    
    @httpHeader("X-Scal-Locations")
    @required
    Locations: String,
}

@output
structure MultipleBackendHeadObjectOutput {
    /// Last modified timestamp
    lastModified: String,
}
