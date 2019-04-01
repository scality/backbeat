# Out-of-band (OOB) Updates from RING to Zenko

## Description

This design document describes the feature of ingesting metadata of objects put
in RING over S3 Connector into Zenko.

The development involves writing a service in Backbeat that allows
ingesting existing objects as well as newly created objects from a remote
S3C/RING into Zenko.

## Purpose

OOB updates from RING feature allows Zenko features on the configured RING
bucket such as CRR, Lifecycle Expiration, Lifecycle Transition, and Metadata
Search. This enables use cases such as

- Cloud D/R i.e. backing up RING to Zenko supported cloud locations
- Policy based transition of objects to cloud locations
- Policy based expiring of existing non-current (archive) versions
- Searching user-defined and system metadata on objects

## Design

The feature allows ingesting objects by the configured RING bucket into the
mapped Zenko bucket.

### Proposed workflow

```
                    +-------------+
                    |             |
    ---------------+| Cloudserver |
    |               |             |
    |               +-------------+   Use S3 API calls to
    |                                list bucket       +----------------------+
    |                                                  |                      |
    |                                                  |     S3 Connector     |
    |                                                  |                      |
    |                              +-----------------> | +------------------+ |
    |                              |                   | |                  | |
    |                              | ----------------> | |  Metadata Server | |
    |             +-----------------+  Obtain list of  | |                  | |
    |             | +-------------+ |   object keys    | +--------^---------+ |
    v             | |             | |                  +-----------|-----------+
+----------+      | |  Kafka      | |                              |
|          | <------+             | |                              |
| Mongo DB |      | +-------------+ |                              |
|          |      | |             | +------------------------------+
+----------+   +-----+ Zookeeper  | |  Using the list of object keys,
               |  | |             | |  get the JSON of each object from Metadata
               |  | +-------------+ |
               |  |                 |
               |  |    Backbeat     |
               |  |                 |
               |  +-----------------+
               |
               |     On startup, check Zookeeper to
               +---> see if a 'sequence id' or `Next Continuation Token` already
                     exists.
```

* Create a new S3C location in Orbit by adding a versioning-enabled S3C bucket
  and `bucketMatch`  option set to true. This ensures that objects appear
  natively without any prefixes.
* Create a bucket in Zenko and select the S3C location created in the above step
  as location constraint that is tagged as `Mirror mode`. This will setup the
  ingestion workflow on the Zenko bucket, allowing 1-1 mapping between the
  bucket defined through Zenko and the RING bucket.
* Cloudserver tags the bucket with the attribute
  `ingestion: { status: 'enabled'}`
* Backbeat will have a service that monitors the MongoDB oplog for the buckets
  that have ingestion enabled using the `ingestion` attribute and sets up
  the ingestion workflow.
* The ingestion process is composed of two phases:
    - **Initial ingestion of existing objects**
    - **Out of band updates for new objects**

#### Initial ingestion of existing objects

* Record the latest sequence ID from the Metadata Server (send a query using the
  `metadataWrapper`) that is part of the S3 connector - this will serve as a
  marker that will be stored as a `log offset` to bookmark the point in journal
  to resume later, after the initial ingestion is completed.
* Send List Object Versions API request to S3C to receive a list of
  object versions in the bucket with details like  `key` and `versionId`.
* Retrieve object JSON from S3C Metadata with the `key` and `versionId`.
* `Next Continuation Token` from listings is used to keep track of listing, to
  recover during unexpected pod/node failures

**Note: Requests to the remote S3C Metadata are performed over S3C's Cloudserver
which proxies the request to Metadata. The request is secure and signed by
AWS V4 authentication scheme.**

#### Out of band updates for new objects

After initial ingestion is completed, Ingestion service will use it's producer
to read from the remote S3C's Metadata and queue the journal entries to Kafka.
Ingestion consumer picks up the entry from Kafka and processes the following
fields for object metadata:

* `owner`
* `ownerDisplayName`
* `owner-id`
* `location` if defined
* `dataStoreName` should match the S3C location name

The values for `owner`, `ownerDisplayName`, and `owner-id` are changed to match
the Orbit user information to allow `GET` operations.
The `location` and `dataStoreName` are changed to allow `PUT` and `DELETE`
operations by the Orbit user.

A Zenko deployment will have 1 `ingestion-populator` pod and multiple
`ingestion-processor` pods. The number of ingestion processor pods depends on
the `nodeCount` value specified during deployment (default is 3). The
`ingestion-populator` pod can support multiple ingestion workflows. The
logReader for each ingestion populator will be setup to source logs for the
specified ingestion bucket.

## Dependencies

* RING with S3 Connector on version 7.4.3 or newer
* Backbeat
* MongoDB
* Kafka
* Zookeeper

## Bucket/Object-Key Parity

The ingestion source is a bucket on RING registered as a storage location with
`bucketMatch` enabled. This ensures an exact match of object names without any
prefixes (unlike the prefix schema used in Zenko locations that host multiple
Zenko 'buckets').

## Avoiding Retro Propagation

The S3C location in `mirror mode` is added as a location constraint to the
Zenko bucket. This allows putting objects in the Zenko bucket, with object's
metadata stored in Zenko, and object's metadata & data stored in S3C.
To avoid re-ingesting these objects back into Zenko, objects will be assigned
`scal-source: zenko-<instance id>` in their in their user-metadata. Any object
versions that have this attribute in their user-metadata will be ignored during
ingestion.

## Background Information for Design

* The `sequence id` from the RING Metadata is in numerical order.
* Zenko only stores metadata, data associated with the object will remain in
  S3C/RING

## Rejected Options

* The option that was originally considered was to ingest all buckets in RING
  in one setting, upon starting Zenko. The ingestion populator and consumers
  would be created automatically and begin listing all buckets and objects on
  the RING.
* For large RINGs with many objects, this would be a very heavy workload that
  could take extremely long periods of time to run to completion.
* This also limits the flexibility for users, if they only wanted to ingest
  and use one bucket with Zenko, they would still have to wait until all buckets
  and objects were processed.
* This implementation also required a fresh install of Zenko, which would bar
  users from making any changes or updates to the ingestion process after the
  initial deployment.
