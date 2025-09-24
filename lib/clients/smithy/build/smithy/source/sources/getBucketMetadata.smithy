$version: "2.0"

namespace com.scality.backbeat

@readonly
@http(method: "GET", uri: "/_/metadata/default/attributes/{Bucket}")
operation GetBucketMetadata {
    input: GetBucketMetadataInput,
    output: GetBucketMetadataOutput
}

structure GetBucketMetadataInput {
    @required
    @httpLabel
    Bucket: String
}

structure GetBucketMetadataOutput {
    /// Access control list for the bucket
    acl: Document,
    /// Name of the bucket
    name: String,
    /// Owner of the bucket
    owner: String,
    /// Display name of the bucket owner
    ownerDisplayName: String,
    /// Creation date of the bucket
    creationDate: String,
    /// Metadata bucket model version
    mdBucketModelVersion: Integer,
    /// Whether this is a transient bucket
    transient: Boolean,
    /// Whether the bucket is marked as deleted
    deleted: Boolean,
    /// Server-side encryption configuration
    serverSideEncryption: Document,
    /// Versioning configuration for the bucket
    versioningConfiguration: Document,
    /// Location constraint for the bucket
    locationConstraint: String,
    /// Read location constraint for the bucket
    readLocationConstraint: String,
    /// CORS configuration for the bucket
    cors: Document,
    /// Replication configuration for the bucket
    replicationConfiguration: Document,
    /// Lifecycle configuration for the bucket
    lifecycleConfiguration: Document,
    /// Unique identifier for the bucket
    uid: String
}
