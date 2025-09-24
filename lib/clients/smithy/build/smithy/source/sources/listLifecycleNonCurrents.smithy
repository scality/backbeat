namespace com.scality.backbeat

use aws.protocols#restJson1

/// List lifecycle non-current objects operation
@readonly
@http(method: "GET", uri: "/_/backbeat/lifecycle/{Bucket}?list-type=noncurrent")
operation ListLifecycleNonCurrents {
    input: ListLifecycleNonCurrentsInput,
    output: ListLifecycleNonCurrentsOutput,
}

/// Input for ListLifecycleNonCurrents operation
structure ListLifecycleNonCurrentsInput {
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
    
    /// Key marker for pagination
    @httpQuery("key-marker")
    KeyMarker: String,
    
    /// Version ID marker for pagination
    @httpQuery("version-id-marker")
    VersionIdMarker: String,
    
    /// Maximum number of keys to return
    @httpQuery("max-keys")
    MaxKeys: Integer,
    
    /// Limits the response to keys that begin with the specified prefix
    @httpQuery("prefix")
    Prefix: String,
}

/// Output for ListLifecycleNonCurrents operation
structure ListLifecycleNonCurrentsOutput {
    /// Limit the response to keys modified prior to before date
    BeforeDate: String,
    
    /// Indicates where in the bucket listing begins
    KeyMarker: String,
    
    /// Marks the last version of the key returned in a truncated response
    VersionIdMarker: String,
    
    /// Flag that indicates whether all results were returned
    IsTruncated: Boolean,
    
    /// Next key marker for pagination
    NextKeyMarker: String,
    
    /// Next version ID marker for pagination
    NextVersionIdMarker: String,
    
    /// Metadata about each object returned
    Contents: ObjectLifecycleList,
    
    /// The bucket name
    Name: String,
    
    /// Keys that begin with the indicated prefix
    Prefix: String,
    
    /// Maximum number of keys returned in the response body
    MaxKeys: Integer,
}
