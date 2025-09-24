namespace com.scality.backbeat

use aws.protocols#restJson1

/// List lifecycle orphan objects operation
@readonly
@http(method: "GET", uri: "/_/backbeat/lifecycle/{Bucket}?list-type=orphan")
operation ListLifecycleOrphans {
    input: ListLifecycleOrphansInput,
    output: ListLifecycleOrphansOutput,
}

/// Input for ListLifecycleOrphans operation (same structure as ListLifecycleCurrents)
structure ListLifecycleOrphansInput {
    /// The bucket name
    @httpLabel
    @required
    Bucket: String,
    
    /// Limit the response to keys modified prior to before date
    @httpQuery("before-date")
    BeforeDate: String,
    
    /// Limit the response to only include keys that are stored outside of the ExcludedDataStoreName
    @httpQuery("excluded-data-store-name")
    ExcludedDataStoreName: String,
    
    /// Encoding type for the response
    @httpQuery("encoding-type")
    EncodingType: String,
    
    /// Marker for pagination
    @httpQuery("marker")
    Marker: String,
    
    /// Maximum number of keys to return
    @httpQuery("max-keys")
    MaxKeys: Integer,
    
    /// Limits the response to keys that begin with the specified prefix
    @httpQuery("prefix")
    Prefix: String,
}

/// Output for ListLifecycleOrphans operation (same structure as ListLifecycleCurrents)
structure ListLifecycleOrphansOutput {
    /// Limit the response to keys modified prior to before date
    BeforeDate: String,
    
    /// Indicates where in the bucket listing begins
    Marker: String,
    
    /// Flag that indicates whether all results were returned
    IsTruncated: Boolean,
    
    /// Next marker for pagination
    NextMarker: String,
    
    /// Metadata about each object returned
    Contents: ObjectLifecycleList,
    
    /// The bucket name
    Name: String,
    
    /// Keys that begin with the indicated prefix
    Prefix: String,
    
    /// Maximum number of keys returned in the response body
    MaxKeys: Integer,
}
