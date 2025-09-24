// smithy-typescript generated code
import { DocumentType as __DocumentType } from "@smithy/types";

/**
 * @public
 */
export interface BatchDeleteLocation {
  /**
   * The data store name where the object is stored
   * @public
   */
  dataStoreName: string | undefined;

  /**
   * The storage key for the object
   * @public
   */
  key: string | undefined;

  /**
   * Size of the object in bytes
   * @public
   */
  size?: number | undefined;

  /**
   * Version ID in the data store
   * @public
   */
  dataStoreVersionId?: string | undefined;
}

/**
 * @public
 */
export interface BatchDeleteInput {
  Bucket: string | undefined;
  Key: string | undefined;
  IfUnmodifiedSince?: string | undefined;
  StorageClass?: string | undefined;
  Tags?: string | undefined;
  ContentType?: string | undefined;
  /**
   * List of locations to delete
   * @public
   */
  Locations?: (BatchDeleteLocation)[] | undefined;
}

/**
 * @public
 */
export interface BatchDeleteOutput {
}

/**
 * @public
 */
export interface DeleteBucketIndexesInput {
  Bucket: string | undefined;
  Body?: Uint8Array | undefined;
}

/**
 * @public
 */
export interface DeleteBucketIndexesOutput {
}

/**
 * @public
 */
export interface DeleteObjectFromExpirationInput {
  Bucket: string | undefined;
  Key: string | undefined;
  VersionId?: string | undefined;
}

/**
 * @public
 */
export interface DeleteObjectFromExpirationOutput {
  /**
   * Version ID of the deleted object
   * @public
   */
  versionId?: string | undefined;
}

/**
 * @public
 */
export interface GetBucketCseqInput {
  Bucket: string | undefined;
}

/**
 * @public
 */
export interface CseqInfo {
  /**
   * Current sequence number
   * @public
   */
  cseq?: number | undefined;
}

/**
 * @public
 */
export interface GetBucketCseqOutput {
  /**
   * List of sequence information
   * @public
   */
  CseqInfo?: (CseqInfo)[] | undefined;
}

/**
 * @public
 */
export interface GetBucketIndexesInput {
  Bucket: string | undefined;
}

/**
 * @public
 */
export interface IndexKey {
  order?: number | undefined;
  key?: string | undefined;
}

/**
 * @public
 */
export interface Index {
  name?: string | undefined;
  keys?: (IndexKey)[] | undefined;
}

/**
 * @public
 */
export interface GetBucketIndexesOutput {
  Indexes?: (Index)[] | undefined;
}

/**
 * @public
 */
export interface GetBucketMetadataInput {
  Bucket: string | undefined;
}

/**
 * @public
 */
export interface GetBucketMetadataOutput {
  /**
   * Access control list for the bucket
   * @public
   */
  acl?: __DocumentType | undefined;

  /**
   * Name of the bucket
   * @public
   */
  name?: string | undefined;

  /**
   * Owner of the bucket
   * @public
   */
  owner?: string | undefined;

  /**
   * Display name of the bucket owner
   * @public
   */
  ownerDisplayName?: string | undefined;

  /**
   * Creation date of the bucket
   * @public
   */
  creationDate?: string | undefined;

  /**
   * Metadata bucket model version
   * @public
   */
  mdBucketModelVersion?: number | undefined;

  /**
   * Whether this is a transient bucket
   * @public
   */
  transient?: boolean | undefined;

  /**
   * Whether the bucket is marked as deleted
   * @public
   */
  deleted?: boolean | undefined;

  /**
   * Server-side encryption configuration
   * @public
   */
  serverSideEncryption?: __DocumentType | undefined;

  /**
   * Versioning configuration for the bucket
   * @public
   */
  versioningConfiguration?: __DocumentType | undefined;

  /**
   * Location constraint for the bucket
   * @public
   */
  locationConstraint?: string | undefined;

  /**
   * Read location constraint for the bucket
   * @public
   */
  readLocationConstraint?: string | undefined;

  /**
   * CORS configuration for the bucket
   * @public
   */
  cors?: __DocumentType | undefined;

  /**
   * Replication configuration for the bucket
   * @public
   */
  replicationConfiguration?: __DocumentType | undefined;

  /**
   * Lifecycle configuration for the bucket
   * @public
   */
  lifecycleConfiguration?: __DocumentType | undefined;

  /**
   * Unique identifier for the bucket
   * @public
   */
  uid?: string | undefined;
}

/**
 * @public
 */
export interface GetMetadataInput {
  Bucket: string | undefined;
  Key: string | undefined;
  VersionId?: string | undefined;
}

/**
 * @public
 */
export interface GetMetadataOutput {
  Body?: string | undefined;
}

/**
 * @public
 */
export interface GetObjectInput {
  Bucket: string | undefined;
  Key: string | undefined;
  VersionId?: string | undefined;
  CanonicalID?: string | undefined;
}

/**
 * @public
 */
export interface GetObjectOutput {
  ContentType?: string | undefined;
  ETag?: string | undefined;
  LastModified?: Date | undefined;
  VersionId?: string | undefined;
  Body?: Uint8Array | undefined;
}

/**
 * @public
 */
export interface GetObjectListInput {
  Bucket: string | undefined;
}

/**
 * @public
 */
export interface ObjectMD {
  key?: string | undefined;
  value?: string | undefined;
}

/**
 * @public
 */
export interface GetObjectListOutput {
  Contents?: (ObjectMD)[] | undefined;
  CommonPrefixes?: (string)[] | undefined;
  IsTruncated?: boolean | undefined;
  Delimiter?: string | undefined;
}

/**
 * @public
 */
export interface GetRaftBucketsInput {
  LogId: string | undefined;
}

/**
 * @public
 */
export interface GetRaftBucketsOutput {
  /**
   * List of bucket names associated with the Raft log
   * @public
   */
  Buckets?: (string)[] | undefined;
}

/**
 * @public
 */
export interface GetRaftIdInput {
  Bucket: string | undefined;
}

/**
 * @public
 */
export interface GetRaftIdOutput {
  RaftId?: string | undefined;
}

/**
 * @public
 */
export interface GetRaftLogInput {
  LogId: string | undefined;
  Begin?: number | undefined;
  Limit?: number | undefined;
  TargetLeader?: boolean | undefined;
}

/**
 * @public
 */
export interface RaftLogInfo {
  /**
   * Starting sequence number
   * @public
   */
  start?: number | undefined;

  /**
   * Current sequence number
   * @public
   */
  cseq?: number | undefined;

  /**
   * Prune sequence number
   * @public
   */
  prune?: number | undefined;
}

/**
 * @public
 */
export interface LogEntryKeyValue {
  /**
   * Entry key
   * @public
   */
  key?: string | undefined;

  /**
   * Entry value
   * @public
   */
  value?: string | undefined;
}

/**
 * @public
 */
export interface RaftLogEntry {
  /**
   * Database name
   * @public
   */
  db?: string | undefined;

  /**
   * List of key-value entries
   * @public
   */
  entries?: (LogEntryKeyValue)[] | undefined;
}

/**
 * @public
 */
export interface GetRaftLogOutput {
  /**
   * Information about the Raft log
   * @public
   */
  info?: RaftLogInfo | undefined;

  /**
   * Log entries
   * @public
   */
  log?: (RaftLogEntry)[] | undefined;
}

/**
 * Input for ListLifecycleCurrents operation
 * @public
 */
export interface ListLifecycleCurrentsInput {
  /**
   * The bucket name
   * @public
   */
  Bucket: string | undefined;

  /**
   * Limit the response to keys modified prior to before date
   * @public
   */
  BeforeDate?: string | undefined;

  /**
   * Limit the response to only include keys that are stored outside of the ExcludedDataStoreName
   * @public
   */
  ExcludedDataStoreName?: string | undefined;

  /**
   * Encoding type for the response
   * @public
   */
  EncodingType?: string | undefined;

  /**
   * Marker for pagination
   * @public
   */
  Marker?: string | undefined;

  /**
   * Maximum number of keys to return
   * @public
   */
  MaxKeys?: number | undefined;

  /**
   * Limits the response to keys that begin with the specified prefix
   * @public
   */
  Prefix?: string | undefined;
}

/**
 * Owner information
 * @public
 */
export interface Owner {
  /**
   * Container for the display name of the owner
   * @public
   */
  DisplayName?: string | undefined;

  /**
   * Container for the ID of the owner
   * @public
   */
  ID?: string | undefined;
}

/**
 * Tag key-value pair
 * @public
 */
export interface Tag {
  /**
   * Name of the tag key
   * @public
   */
  Key: string | undefined;

  /**
   * Value of the tag
   * @public
   */
  Value: string | undefined;
}

/**
 * Object lifecycle information
 * @public
 */
export interface ObjectLifecycle {
  /**
   * The name that you assign to an object
   * @public
   */
  Key?: string | undefined;

  /**
   * Creation date of the object
   * @public
   */
  LastModified?: string | undefined;

  /**
   * The entity tag is a hash of the object
   * @public
   */
  ETag?: string | undefined;

  /**
   * The owner of the object
   * @public
   */
  Owner?: Owner | undefined;

  /**
   * Size in bytes of the object
   * @public
   */
  Size?: number | undefined;

  /**
   * The class of storage used to store the object
   * @public
   */
  StorageClass?: string | undefined;

  /**
   * Contains the tag set
   * @public
   */
  TagSet?: (Tag)[] | undefined;

  /**
   * Contains the stale date
   * @public
   */
  staleDate?: string | undefined;

  /**
   * Version ID
   * @public
   */
  VersionId?: string | undefined;

  /**
   * The data location name
   * @public
   */
  DataStoreName?: string | undefined;

  /**
   * List type
   * @public
   */
  ListType?: string | undefined;
}

/**
 * Output for ListLifecycleCurrents operation
 * @public
 */
export interface ListLifecycleCurrentsOutput {
  /**
   * Limit the response to keys modified prior to before date
   * @public
   */
  BeforeDate?: string | undefined;

  /**
   * Indicates where in the bucket listing begins
   * @public
   */
  Marker?: string | undefined;

  /**
   * Flag that indicates whether all results were returned
   * @public
   */
  IsTruncated?: boolean | undefined;

  /**
   * Next marker for pagination
   * @public
   */
  NextMarker?: string | undefined;

  /**
   * Metadata about each object returned
   * @public
   */
  Contents?: (ObjectLifecycle)[] | undefined;

  /**
   * The bucket name
   * @public
   */
  Name?: string | undefined;

  /**
   * Keys that begin with the indicated prefix
   * @public
   */
  Prefix?: string | undefined;

  /**
   * Maximum number of keys returned in the response body
   * @public
   */
  MaxKeys?: number | undefined;
}

/**
 * Input for ListLifecycleNonCurrents operation
 * @public
 */
export interface ListLifecycleNonCurrentsInput {
  /**
   * The bucket name
   * @public
   */
  Bucket: string | undefined;

  /**
   * Limit the response to keys modified prior to before date
   * @public
   */
  BeforeDate?: string | undefined;

  /**
   * Limit the response to only include keys that are stored outside of the ExcludedDataStoreName
   * @public
   */
  ExcludedDataStoreName?: string | undefined;

  /**
   * Encoding type for the response
   * @public
   */
  EncodingType?: string | undefined;

  /**
   * Key marker for pagination
   * @public
   */
  KeyMarker?: string | undefined;

  /**
   * Version ID marker for pagination
   * @public
   */
  VersionIdMarker?: string | undefined;

  /**
   * Maximum number of keys to return
   * @public
   */
  MaxKeys?: number | undefined;

  /**
   * Limits the response to keys that begin with the specified prefix
   * @public
   */
  Prefix?: string | undefined;
}

/**
 * Output for ListLifecycleNonCurrents operation
 * @public
 */
export interface ListLifecycleNonCurrentsOutput {
  /**
   * Limit the response to keys modified prior to before date
   * @public
   */
  BeforeDate?: string | undefined;

  /**
   * Indicates where in the bucket listing begins
   * @public
   */
  KeyMarker?: string | undefined;

  /**
   * Marks the last version of the key returned in a truncated response
   * @public
   */
  VersionIdMarker?: string | undefined;

  /**
   * Flag that indicates whether all results were returned
   * @public
   */
  IsTruncated?: boolean | undefined;

  /**
   * Next key marker for pagination
   * @public
   */
  NextKeyMarker?: string | undefined;

  /**
   * Next version ID marker for pagination
   * @public
   */
  NextVersionIdMarker?: string | undefined;

  /**
   * Metadata about each object returned
   * @public
   */
  Contents?: (ObjectLifecycle)[] | undefined;

  /**
   * The bucket name
   * @public
   */
  Name?: string | undefined;

  /**
   * Keys that begin with the indicated prefix
   * @public
   */
  Prefix?: string | undefined;

  /**
   * Maximum number of keys returned in the response body
   * @public
   */
  MaxKeys?: number | undefined;
}

/**
 * Input for ListLifecycleOrphans operation (same structure as ListLifecycleCurrents)
 * @public
 */
export interface ListLifecycleOrphansInput {
  /**
   * The bucket name
   * @public
   */
  Bucket: string | undefined;

  /**
   * Limit the response to keys modified prior to before date
   * @public
   */
  BeforeDate?: string | undefined;

  /**
   * Limit the response to only include keys that are stored outside of the ExcludedDataStoreName
   * @public
   */
  ExcludedDataStoreName?: string | undefined;

  /**
   * Encoding type for the response
   * @public
   */
  EncodingType?: string | undefined;

  /**
   * Marker for pagination
   * @public
   */
  Marker?: string | undefined;

  /**
   * Maximum number of keys to return
   * @public
   */
  MaxKeys?: number | undefined;

  /**
   * Limits the response to keys that begin with the specified prefix
   * @public
   */
  Prefix?: string | undefined;
}

/**
 * Output for ListLifecycleOrphans operation (same structure as ListLifecycleCurrents)
 * @public
 */
export interface ListLifecycleOrphansOutput {
  /**
   * Limit the response to keys modified prior to before date
   * @public
   */
  BeforeDate?: string | undefined;

  /**
   * Indicates where in the bucket listing begins
   * @public
   */
  Marker?: string | undefined;

  /**
   * Flag that indicates whether all results were returned
   * @public
   */
  IsTruncated?: boolean | undefined;

  /**
   * Next marker for pagination
   * @public
   */
  NextMarker?: string | undefined;

  /**
   * Metadata about each object returned
   * @public
   */
  Contents?: (ObjectLifecycle)[] | undefined;

  /**
   * The bucket name
   * @public
   */
  Name?: string | undefined;

  /**
   * Keys that begin with the indicated prefix
   * @public
   */
  Prefix?: string | undefined;

  /**
   * Maximum number of keys returned in the response body
   * @public
   */
  MaxKeys?: number | undefined;
}

/**
 * @public
 */
export interface MultipleBackendAbortMPUInput {
  Bucket: string | undefined;
  Key: string | undefined;
  StorageType?: string | undefined;
  StorageClass: string | undefined;
  UploadId?: string | undefined;
}

/**
 * @public
 */
export interface MultipleBackendAbortMPUOutput {
}

/**
 * @public
 */
export interface MultipleBackendCompleteMPUInput {
  Bucket: string | undefined;
  Key: string | undefined;
  StorageType?: string | undefined;
  StorageClass: string | undefined;
  VersionId?: string | undefined;
  ContentType?: string | undefined;
  UserMetaData?: string | undefined;
  CacheControl?: string | undefined;
  ContentDisposition?: string | undefined;
  ContentEncoding?: string | undefined;
  UploadId?: string | undefined;
  Tags?: string | undefined;
  Body?: Uint8Array | undefined;
}

/**
 * @public
 */
export interface LocationMDObj {
  /**
   * Storage key for this location
   * @public
   */
  key?: string | undefined;

  /**
   * Size of the data stored at this location
   * @public
   */
  size?: number | undefined;

  /**
   * Start position/offset for this data segment
   * @public
   */
  start?: number | undefined;

  /**
   * Name of the data store where this is located
   * @public
   */
  dataStoreName?: string | undefined;

  /**
   * Type of the data store (e.g., file, mem, etc.)
   * @public
   */
  dataStoreType?: string | undefined;

  /**
   * ETag from the data store for this location
   * @public
   */
  dataStoreETag?: string | undefined;

  /**
   * Version ID in the data store for this location
   * @public
   */
  dataStoreVersionId?: string | undefined;
}

/**
 * @public
 */
export interface MultipleBackendCompleteMPUOutput {
  /**
   * Version ID of the completed object
   * @public
   */
  versionId?: string | undefined;

  /**
   * Location information
   * @public
   */
  location?: (LocationMDObj)[] | undefined;
}

/**
 * @public
 */
export interface MultipleBackendDeleteObjectInput {
  Bucket: string | undefined;
  Key: string | undefined;
  StorageType?: string | undefined;
  StorageClass: string | undefined;
}

/**
 * @public
 */
export interface MultipleBackendDeleteObjectOutput {
  versionId?: string | undefined;
}

/**
 * @public
 */
export interface MultipleBackendDeleteObjectTaggingInput {
  Bucket: string | undefined;
  Key: string | undefined;
  StorageClass: string | undefined;
  StorageType?: string | undefined;
  DataStoreVersionId?: string | undefined;
  SourceBucket?: string | undefined;
  SourceVersionId?: string | undefined;
  ReplicationEndpointSite?: string | undefined;
  /**
   * TODO : REVIEW THIS FIELD
   * @public
   */
  Body?: Uint8Array | undefined;
}

/**
 * @public
 */
export interface MultipleBackendDeleteObjectTaggingOutput {
  /**
   * Version ID of the object after tag removal
   * @public
   */
  versionId?: string | undefined;
}

/**
 * @public
 */
export interface MultipleBackendHeadObjectInput {
  Bucket: string | undefined;
  Key: string | undefined;
  Locations: string | undefined;
}

/**
 * @public
 */
export interface MultipleBackendHeadObjectOutput {
  /**
   * Last modified timestamp
   * @public
   */
  lastModified?: string | undefined;
}

/**
 * @public
 */
export interface MultipleBackendInitiateMPUInput {
  Bucket: string | undefined;
  Key: string | undefined;
  StorageClass: string | undefined;
  VersionId?: string | undefined;
  StorageType?: string | undefined;
  ContentType?: string | undefined;
  UserMetaData?: string | undefined;
  CacheControl?: string | undefined;
  ContentDisposition?: string | undefined;
  ContentEncoding?: string | undefined;
  Tags?: string | undefined;
  Body?: Uint8Array | undefined;
}

/**
 * @public
 */
export interface MultipleBackendInitiateMPUOutput {
  /**
   * Upload ID for the multipart upload
   * @public
   */
  uploadId?: string | undefined;
}

/**
 * @public
 */
export interface MultipleBackendPutMPUPartInput {
  Bucket: string | undefined;
  Key: string | undefined;
  StorageType?: string | undefined;
  StorageClass: string | undefined;
  PartNumber?: number | undefined;
  UploadId?: string | undefined;
  Body?: Uint8Array | undefined;
}

/**
 * @public
 */
export interface MultipleBackendPutMPUPartOutput {
  /**
   * Part number
   * @public
   */
  partNumber?: number | undefined;

  /**
   * ETag of the uploaded part
   * @public
   */
  ETag?: string | undefined;

  /**
   * Number of sub-parts
   * @public
   */
  numberSubParts?: number | undefined;
}

/**
 * @public
 */
export interface MultipleBackendPutObjectInput {
  Bucket: string | undefined;
  Key: string | undefined;
  ContentMD5?: string | undefined;
  ContentType?: string | undefined;
  UserMetaData?: string | undefined;
  CacheControl?: string | undefined;
  ContentDisposition?: string | undefined;
  ContentEncoding?: string | undefined;
  CanonicalID?: string | undefined;
  StorageClass: string | undefined;
  StorageType?: string | undefined;
  VersionId?: string | undefined;
  Tags?: string | undefined;
  Body?: Uint8Array | undefined;
}

/**
 * @public
 */
export interface MultipleBackendPutObjectOutput {
  /**
   * Version ID of the stored object
   * @public
   */
  versionId?: string | undefined;

  /**
   * List of storage locations where the object was stored
   * @public
   */
  location?: (LocationMDObj)[] | undefined;
}

/**
 * @public
 */
export interface MultipleBackendPutObjectTaggingInput {
  Bucket: string | undefined;
  Key: string | undefined;
  StorageType?: string | undefined;
  StorageClass: string | undefined;
  DataStoreVersionId?: string | undefined;
  Tags?: string | undefined;
  SourceBucket?: string | undefined;
  SourceVersionId?: string | undefined;
  ReplicationEndpointSite?: string | undefined;
  Body?: Uint8Array | undefined;
}

/**
 * @public
 */
export interface MultipleBackendPutObjectTaggingOutput {
  /**
   * Version ID of the tagged object
   * @public
   */
  versionId?: string | undefined;
}

/**
 * @public
 */
export interface PutBucketIndexesInput {
  Bucket: string | undefined;
  Body?: Uint8Array | undefined;
}

/**
 * @public
 */
export interface PutBucketIndexesOutput {
}

/**
 * @public
 */
export interface PutDataInput {
  Bucket: string | undefined;
  Key: string | undefined;
  ContentMD5?: string | undefined;
  CanonicalID?: string | undefined;
  VersioningRequired?: boolean | undefined;
  Body?: Uint8Array | undefined;
}

/**
 * @public
 */
export interface PutDataOutput {
  LocationsData?: __DocumentType | undefined;
}

/**
 * @public
 */
export interface PutMetadataInput {
  Bucket: string | undefined;
  Key: string | undefined;
  VersionId?: string | undefined;
  AccountId?: string | undefined;
  ContentMD5?: string | undefined;
  ReplicationContent?: string | undefined;
  VersioningRequired?: boolean | undefined;
  Body?: Uint8Array | undefined;
}

/**
 * @public
 */
export interface PutMetadataOutput {
  /**
   * Version ID of the stored metadata
   * @public
   */
  versionId?: string | undefined;
}
