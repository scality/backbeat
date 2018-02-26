# CRR from CloudServer to public cloud - Workflow

## Installation

An endpoint ('awsbackend', for example) will be defined in the [location
configuration](https://github.com/scality/S3/blob/master/locationConfig.json) of
[CloudServer](https://github.com/scality/S3). Each location constraint in the
configuration file has properties needed for performing operations for the
location. For example, if replicating to AWS S3, the properties will include the
bucket name, region, and credentials for AWS S3. This enables the multiple
backend feature in CloudServer that Backbeat uses for CRR to a public cloud.

Replication endpoints are defined in the config for both Backbeat and
CloudServer in Federation's [group_vars/all](https://github.com/scality/Federation/blob/master/env/client-template/group_vars/all) file:

```
env_replication_endpoints:
  site: awsbackend
  type: aws_s3
  site: azurebackend
  type: azure
```

During installation, these values are set in the destination `bootstrapList` in
Backbeat's [configuration
file](https://github.com/scality/backbeat/blob/master/conf/config.json) and the
`replicationEndpoints` value in CloudServer's [configuration
file](https://github.com/scality/S3/blob/master/config.json).

### Operations

Below is a sequence diagram starting from the user's PUT object request on a
bucket with replication enabled.

![design](/res/queue-processor-sequence-diagram.png)

#### CloudServer: PUT Bucket Replication Request

* CloudServer receives the request and checks the storage class set in the
  bucket replication configuration (see the
  `StorageClass` property
  [here](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#putBucketReplication-property)). If defined, this property is a comma-separated list of location
  constraints. For example, if replicating to two locations 'awsbackend' and
  'azurebackend' (defined in the [location
  configuration](https://github.com/scality/S3/blob/master/locationConfig.json)
  of [CloudServer](https://github.com/scality/S3)), `StorageClass` would take
  the form `'awsbackend,azurebackend'`)

* Check the following in the `replicationEndpoints` value (defined in        [config.json](https://github.com/scality/backbeat/blob/master/conf/config.json)
):
    * The given storage class(es) must match a site name
    * If the site matches, then the site should either have a `type` or
      `servers` property
    * If the storage class is omitted, the default `replicationEndpoints` site
      is used
    * If `type` (e.g., `'aws_s3'`) is defined, CloudServer writes this to the
      objects' replication information property `storageType`

#### CloudServer: PUT object requests on bucket with replication enabled

* CloudServer receives the request and checks if bucket has replication enabled

* If replication is enabled, `storageType` and `storageClass` are added to the
  object's `replicationInfo` property and the object's overall `status` is set
  to PENDING. An object for each endpoint is pushed to the
  `replicationInfo.backends` property, each with its own `status` property set
  to PENDING.

#### Overview of CRR processing

* Queue Populator publishes the metadata entry of the object in Kafka since it
  has `replicationStatus` PENDING

* A queue processor is running for each `site` defined in
  `env_replication_endpoints`, and is subscribed to the Kafka topic published to
  in the previous step. The QueueProcessor reads the entry from Kafka and sees
  that the object's storage class includes its designated site (for example,
  `awsbackend`).

* The queue processor spawns a MultipleBackendTask to use the Backbeat routes
  in CloudServer to put the object to the public cloud (in our example, AWS S3).

#### MultipleBackendTask and CloudServer's Backbeat routes

* Upon acquiring an entry to replicate, the task sends a GET Object ?versionId
  request to CloudServer

* The data stream is piped to PUT /\_/backbeat data request with header
  `x-scal-replication-storage-type: aws_s3` in CloudServer's [backbeat routes](
   https://github.com/scality/S3/blob/master/lib/routes/routeBackbeat.js).
   If the request has a public cloud header (e.g., `aws_s3`), use the
   corresponding `multiplebackenddata` PUT route.

* CloudServer PUT /\_/backbeat data performs the PUT operation to the public
  cloud using the given location constraint

* The `multiplebackenddata` route puts the object in AWS S3 with extra user
  metadata `x-amz-scal-replication-status: REPLICA` and stores CloudServer's
  versionId for the entry in `x-amz-scal-version-id`

* If we get 200 from the public cloud, CloudServer responds back with 200 to
  Backbeat MultipleBackendTask with the `x-amz-version-id` it received from AWS
  in the header/body

* Upon receiving 200 from the CloudServer Backbeat route, MultipleBackendTask
  reads `x-amz-scal-version-id` from the header and hands off setting metadata
  properties of the source object to the replication status processor

#### ReplicationStatusProcessor

* Update the Kafka replication status topic with the overall status of the
  object's replication. (For example, if all backends are COMPLETED, the overall
  status is also COMPLETED, or if one backend is still PENDING while others have
  COMPLETED, the overall status is PROCESSING.)

* Send PUT /\_/backbeat metadata request on the source object to update the
  backend's site property `status` to COMPLETED or FAILED, and the version ID of
  the destination object (i.e., the backend's `dataStoreVersionId` property)
  `x-amz-scal-version-id` to the value received in the header
