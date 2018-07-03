# Cross-Region Replication (CRR) Retry

## Description

This feature offers a way for users to monitor and retry failed cross-region
replication (CRR) operations. It enables users to retrieve a list of failed
operations and retry specific CRR operations.

## Requirements

* The listing of failed operations.
* The retry of a specific failed operation.

## Design

A RESTful API will expose methods for users to list and retry failed operations.
See [Explanations](#explanations) for more detail.

## Definition of API

* GET `/_/crr/failed?marker=marker`

    This GET request retrieves a listing of all failed operations. This
    operation is useful if you're interested in whether any CRR operations have
    failed and want the entire listing.

    Response that is not truncated:

    ```sh
    {
        IsTruncated: false,
        Versions: [{
            Bucket: <bucket>,
            Key: <key>,
            VersionId: <version-id>,
            StorageClass: <site>,
            Size: <size>,
            LastModified: <last-modified>,
        }]
    }
    ```

    Response that is truncated:

    ```sh
    {
        IsTruncated: true,
        NextMarker: <next-marker>,
        Versions: [{
            Bucket: <bucket>,
            Key: <key>,
            VersionId: <version-id>,
            StorageClass: <site>,
            Size: <size>,
            LastModified: <last-modified>,
        },
        ...
        ]
    }
    ```

* GET `/_/crr/failed?bucket=<bucket>&key=<key>&version-id=<version-id>`

    This GET request retrieves a listing of all failed operations for a specific
    object version. This operation is useful if you're interested in monitoring
    the replication status of a specific object.

    Response:

    ```sh
    {
        IsTruncated: false,
        Versions: [{
            Bucket: <bucket>,
            Key: <key>,
            VersionId: <version-id>,
            StorageClass: <site>,
            Size: <size>,
            LastModified: <last-modified>,
        }]
    }
    ```

    NOTE: The marker query parameter is not supported for this route because we
    anticipate that any replication rule will not include more than 1000 sites.

* POST `/_/crr/failed`

    This POST request retries a set of failed operations.

    Request Body:

    ```sh
    [{
        Bucket: <bucket>,
        Key: <key>,
        VersionId: <version-id>,
        StorageClass: <site>,
    }]
    ```

    Response:

    ```sh
    [{
        Bucket: <bucket>,
        Key: <key>,
        VersionId: <version-id>,
        StorageClass: <site>,
        Size: <size>,
        LastModified: <last-modified>,
        ReplicationStatus: 'PENDING',
    }]
    ```

## Explanations

### Redis

The replication status processor pushes a Kafka entry to a topic dedicated for
queuing failed replication operations. It does so for each site with a FAILED
status.

Each Kafka entry takes the following form:

```
{
    key: <bucket>:<key>:<versionId>:<site>
}
```

A Kafka consumer is subscribed to the topic. For each entry consumed, a Redis
key is set using the following schema (the key has a configurable expiry time,
the default of which is 24 hours):

```
bb:crr:failed:<bucket>:<key>:<versionId>:<site>
```

The following diagram shows the steps leading up to setting the key in Redis.

![design](/res/object-failure-scenario.png)

### Listing

When a GET request is received for the list all route (i.e. `/_/crr/failed`),
all Redis keys beginning with `bb:crr:failed` will be retrieved and sent as a
response defined in [Definition of API](#definition-of-api).

When a GET request is received for the list version route (i.e.
`/_/crr/failed/<bucket>/<key>/<versionId>`), all Redis keys beginning with
`bb:crr:failed:<bucket>:<key>:<versionId>` will be retrieved and sent as a
response defined in [Definition of API](#definition-of-api).

### Retry

When a POST request is received for the route `/_/crr/failed` the following
steps occur:

1. Get the object version's metadata from S3's GET metadata route.

2. Push a new Kafka entry to the replication status topic with the source
   object's metadata site status set to PENDING. This will cause the replication
   status processor to update the source object's metadata to indicate the
   site's replication status is now PENDING.

3. Push a new Kafka entry to the replication topic with the source object's
   metadata site status set to PENDING. This will cause the replication
   processor responsible for performing operations for the given site to attempt
   the replication operation again.

4. Delete the Redis key for the operation which is being retried.

The following diagram shows the above steps in the context of a successful retry.

![design](/res/object-retry-scenario.png)

## Dependencies

* Zookeeper
* Kafka
* Redis

## Operational Considerations

* When retrying a previous object version (i.e., not the master version), it
  will overwrite the destination master version if replicating to a public
  cloud.

* Using Redis' [SCAN](https://redis.io/commands/scan), with a default
  [COUNT](https://redis.io/commands/scan#the-count-option) of 1000, allows us to
  search for particular key matches and search all keys. However,
  [SCAN](https://redis.io/commands/scan) does not guarantee any number of
  elements being returned at each iteration so we cannot guarantee a
  pre-defined number of failed version in the response listing.

## Rejected Options

* Creating Zookeeper nodes with the failed object's metadata for maintaining the
  list of failed entries instead of using Redis.

* Storing the failed object metadata as the value of the Redis key. While this
  approach avoids an additional HTTP call to S3, the memory usage improvement in
  Redis is significant.

* Using a hash or list data structure in Redis to store the keys. While
  potentially more memory efficient, we want to leverage Redis' expiry feature
  which is not supported for these data types.

* Setting the key immediately in Redis in the replication status processor. We
  have opted instead to push the key to a dedicated topic and have a consumer of
  that topic handle setting the Redis key. This design decouples the replication
  status processor from Redis, allowing for a reliable history of failed
  operations in the case that the Redis server is offline.