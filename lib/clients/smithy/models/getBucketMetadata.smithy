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
    acl: AclObj,
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
    serverSideEncryption: ServerSideEncryptionMap,
    /// Versioning configuration for the bucket
    versioningConfiguration: VersioningConfigurationObj,
    /// Location constraint for the bucket
    locationConstraint: String,
    /// Read location constraint for the bucket
    readLocationConstraint: String,
    /// CORS configuration for the bucket
    cors: CorsListObj,
    /// Replication configuration for the bucket
    replicationConfiguration: ReplicationConfigurationObj,
    /// Lifecycle configuration for the bucket
    lifecycleConfiguration: LifecycleConfigurationObj,
    /// Unique identifier for the bucket
    uid: String
}

// Define the structured types based on the JSON API
structure AclObj {
    /// Canned ACL setting
    Canned: String,
    /// List of users with FULL_CONTROL permission
    FULL_CONTROL: StringList,
    /// List of users with WRITE permission
    WRITE: StringList,
    /// List of users with WRITE_ACP permission
    WRITE_ACP: StringList,
    /// List of users with READ permission
    READ: StringList,
    /// List of users with READ_ACP permission
    READ_ACP: StringList
}

list StringList {
    member: String
}

map ServerSideEncryptionMap {
    key: String,
    value: String
}

map VersioningConfigurationObj {
    key: String,
    value: String
}

list CorsListObj {
    member: CorsObj
}

map CorsObj {
    key: String,
    value: String
}

map ReplicationConfigurationObj {
    key: String,
    value: String
}

structure LifecycleConfigurationObj {
    /// List of lifecycle rules
    Rules: LifecycleRuleList
}

list LifecycleRuleList {
    member: LCRuleObj
}

structure LCRuleObj {
    /// Unique identifier for the rule
    ID: String,
    /// Whether the rule is enabled or disabled
    Status: LifecycleRuleStatus,
    /// Prefix for objects to which the rule applies
    Prefix: String,
    /// Expiration configuration
    Expiration: ExpirationConfiguration
}

enum LifecycleRuleStatus {
    ENABLED = "Enabled",
    DISABLED = "Disabled"
}

structure ExpirationConfiguration {
    /// Number of days after which the object expires
    Days: Integer
}
