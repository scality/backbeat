namespace com.scality.backbeat

use aws.protocols#restJson1

/// List lifecycle current objects operation
@readonly
@http(method: "GET", uri: "/_/backbeat/lifecycle/{Bucket}?list-type=current")
operation ListLifecycleCurrents {
    input: ListLifecycleCurrentsInput,
    output: ListLifecycleCurrentsOutput,
}

/// Input for ListLifecycleCurrents operation
structure ListLifecycleCurrentsInput {
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
    EncodingType: EncodingType,
    
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

/// Output for ListLifecycleCurrents operation
structure ListLifecycleCurrentsOutput {
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

/// List of ObjectLifecycle objects
list ObjectLifecycleList {
    member: ObjectLifecycle
}

/// Object lifecycle information
structure ObjectLifecycle {
    /// The name that you assign to an object
    Key: String,
    
    /// Creation date of the object
    LastModified: String,
    
    /// The entity tag is a hash of the object
    ETag: String,
    
    /// The owner of the object
    Owner: Owner,
    
    /// Size in bytes of the object
    Size: Integer,
    
    /// The class of storage used to store the object
    StorageClass: String,
    
    /// Contains the tag set
    TagSet: TagSet,
    
    /// Contains the stale date
    staleDate: String,
    
    /// Version ID
    VersionId: String,
    
    /// The data location name
    DataStoreName: String,
    
    /// List type
    ListType: String,
}

/// Owner information
structure Owner {
    /// Container for the display name of the owner
    DisplayName: String,
    
    /// Container for the ID of the owner
    ID: String,
}

/// List of tags
list TagSet {
    member: Tag
}

/// Tag key-value pair
structure Tag {
    /// Name of the tag key
    @required
    Key: String,
    
    /// Value of the tag
    @required
    Value: String,
}

/// Encoding type string with allowed values
string EncodingType
