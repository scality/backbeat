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

* GET `/_/crr/failed`

    This GET request retrieves a listing of all failed operations. This
    operation is useful if you're interested in whether any CRR operations have
    failed and want the entire listing.

    Response:

    ```sh
    [{
        bucket: <bucket>,
        key: <key>,
        versionId: <versionId>,
        site: <site>
    }]
    ```

* GET `/_/crr/failed/<bucket>/<key>/<versionId>`

    This GET request retrieves a listing of all failed operations for a specific
    object version. This operation is useful if you're interested in monitoring
    the replication status of a specific object.

    Response:

    ```sh
    [{
        bucket: <bucket>,
        key: <key>,
        versionId: <versionId>,
        site: <site>
    }]
    ```

* POST `/_/crr/failed/retry`

    This POST request retries a failed operation.

    Request Body:

    ```sh
    [{
        bucket: <bucket>,
        key: <key>,
        versionId: <versionId>,
        site: <site>
    }]
    ```

    Response:

    ```sh
    [{
        bucket: <bucket>,
        key: <key>,
        versionId: <versionId>,
        site: <site>,
        status: PENDING
    }]
    ```

## Explanations

### Redis

Redis will contain a hash with the key:

```
backbeat:crr:failed
```

The replication status processor sets a hash field for any backend with a FAILED
status using the following schema:

```sh
<bucket>:<key>:<versionId>:<site>
```

The value of the Redis hash field is the object's metadata at the time of failure.

### Listing

When a GET request is received for the listing route (e.g.,
`/_/crr/failed`), all Redis keys beginning with `backbeat:crr:failed` will
be retrieved and sent as a response defined in [Definition of
API](#definition-of-api).

### Retry

When a POST request is received for the retry route `/_/crr/failed/retry` the
following steps occur:

1. Get the object's metadata stored by the Redis key (the schema of which is
   defined in [Redis](#redis)).

2. Delete the Redis key for the operation which is being retried.

3. Push a new Kafka entry to the replication status topic with the source
   object's metadata site status set to PENDING. This will cause the replication
   status processor to update the source object's metadata to indicate the
   site's replication status is now PENDING.

4. Push a new Kafka entry to the replication topic with the source object's
   metadata site status set to PENDING. This will cause the replication
   processor responsible for performing operations for the given site to attempt
   the replication operation again.

## Dependencies

* Zookeeper
* Kafka
* Redis

## Operational Considerations

* When retrying a previous object version (i.e., not the master version), it
  will overwrite the destination master version if replicating to a public
  cloud.

## Rejected Options

* Creating Zookeeper nodes with the failed object's metadata for maintaining the
  list of failed entries instead of using Redis.