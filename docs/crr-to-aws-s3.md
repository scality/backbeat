# CRR from CloudServer to AWS S3 - Workflow

## Installation

AWS S3 endpoint (aws-us-east-1 for example) will be defined as a location
constraint in the locationConstraint section of
[CloudServer](https://github.com/scality/S3). The locationConstraint config has
properties to set the bucket name, region, credentials for AWS S3.This enables
multiple backend feature in CloudServer which Backbeat will leverage to CRR to
AWS S3.

replication endpoints will be defined in the config for both Backbeat and
CloudServer

```
env_replication_endpoints:
  site: aws-us-east-1
  type: aws_s3
```

### Operations

#### CloudServer: PUT Bucket Replication Request

- CloudServer receives the request and checks the Storage Class set in the
  request

- Check in replicationEndpoints defined in config.json
    - It should match one of the sites
    - If site matches, then it should either have type or servers property
    - If type is aws_s3, CloudServer writes this to the bucket's replication
        config property, `storageType`

#### CloudServer: PUT object requests on bucket with replication enabled

* CloudServer receives the request and checks if bucket has replication enabled

* If replication is enabled, storageType and storageClass  are added to the
  object's replicationInfo property and replicationStatus is set to PENDING.

#### Overview of CRR processing

* Queue Populator populates the metadata entry of the object in Kafka since it
  has replicationStatus `PENDING`

* QueueProcessor reads the entry from Kafka and sees that storageType is
  `aws_s3`

* QueueProcessor spawns AWSReplicationTask to use the Backbeat routes in
  CloudServer to put objects in AWS S3

#### AWS Replication Task

* Upon acquiring an entry to replicate, the task sends a GET Object ?versionId
  request to CloudServer

* The steam is piped to PUT /_/backbeat Data request with header
  `x-scal-replication-storage-type: aws_s3`

* Upon receiving 200, read `x-amz-scal-version-id` from the header and send PUT
  /_/backbeat MD request on source to update the properties replicationStatus to
  COMPLETED, x-amz-scal-version-id to the value received in the header.

#### CloudServer PUT /_/backbeat Data for AWS route

* If the request has header `x-scal-replication-storage-type: aws_s3`, use the
  multipleDataBackend.

* MultipleDataBackend puts in AWS S3 with extra user metadata
  `x-amz-scal-replication-status: REPLICA` and stores CloudServer's versionId for
  the entry in `x-amz-scal-version-id`

* If we get 200 from AWS, CloudServer responds back with 200 to Backbeat with
  x-amz-version-id it received from AWS in the header/body

* Backbeat updates the source object's metadata to store AWS' version id and set
  replication status as `COMPLETED`
