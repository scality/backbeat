# Object Lifecycle Management

## OVERVIEW

The purpose is to apply various lifecycle actions to objects after a
certain time has passed since object creation, per lifecycle rules as
specified in AWS S3 APIs.

### EXPIRATION

We support expiration of versioned or non-versioned objects, when a
number of days have passed since their creation.

One important use case is to allow automatic deletion of older
versions of versioned objects to reclaim storage space.

## DESIGN

The implementation of lifecycle in S3 connector comprises several
components which, by working together provide lifecycle
functionalities.

Most components rely on Kafka and Zookeeper to pass state to each
other.

### Workflow Overview

Here's an example workflow that expires version *v1* of object *obj*
from bucket *foobucket*:

```
 __________________
/                  \
|   S3 client      |
\__________________/
       |
       | PUT /foobucket?lifecycle
       |
 ______v___________
/                  \         ___________
|                  |        /           \
| S3 Connector     |--------|  METADATA |
|                  |        |    log    |
|                  |        \___________/
\__________________/              |
    ^    ^                        |
    |    |                        | entry: "PUT /foobucket?lifecycle"
    |    |                        |
    |    |                 _______v_________                 ___________
    |    |                /                 \  foobucket    /           \
    |    |                | Queue Populator |-------------->|           |
    |    |                \_________________/               | ZOOKEEPER |
    |    |                                   foobucket      |           |
    |    |                              /-------------------\___________/
    |    |                 _____________v___
    |    |                /                 \
    |    |                |   Conductor     | foobucket@null ___________
    |    |                \_________________/-------------->/           \
    |    |                                                  |           |
    |    | GET /foobucket?lifecycle                         |   KAFKA   |
    |    | list foobucket (max n) => obj@v1                 |           |
    |    |                 _________________                |           |
    |    |                /                 \<--------------\___________/
    |    \----------------|   Producer      |foobucket@marker  ^    ^  |
    |                     |                 |                  |    |  |
    |                     \_________________/------------------/    |  |
    |                                  |    expire foobucket/obj@v1 |  |
    |                                  |                            |  |
    |                                  \----------------------------/  |
    | DELETE foobucket/obj@v1               foobucket@(marker+n)       |
    |                      _________________                           |
    \---------------------/                 \<-------------------------/
                          |   Consumer      |  expire foobucket/obj@v1
                          \_________________/

```

### Lists of actors

#### Lifecycle Components

* Queue populator (extended as explained below)
* Conductor
* Producer
* Consumer

#### Kafka topics for lifecycle

* backbeat-lifecycle-bucket-tasks
* backbeat-lifecycle-object-tasks

#### Zookeeper paths

* /[chroot_path]/lifecycle/data/buckets/${ownerId}:${bucket}
* /[chroot_path]/lifecycle/run/backlog-metrics/${topic}/${partition}/topic
* /[chroot_path]/lifecycle/run/backlog-metrics/${topic}/${partition}/consumers/${groupId}

### S3 Connector

The S3 API provides three calls to manage lifecycle properties per bucket:

* PUT Bucket Lifecycle
* GET Bucket Lifecycle
* DELETE Bucket Lifecycle

See AWS specs for details on the protocol format.

Those calls essentially manage bucket attributes related to lifecycle
behavior, which are stored as part of bucket metadata.

### Queue Populator

The queue populator is an existing component of backbeat that reads
the metadata log and populates entries for CRR.

It is extended for lifecycle management, to maintain a list of buckets
which have to be processed for lifecycle actions, so that only those
buckets are processed.

The list of buckets is stored in zookeeper nodes with the path:

```
[/chroot_path]/lifecycle/data/buckets/${ownerId}:${bucket}
```

* **ownerId** is the canonical ID of the bucket owner
* **bucket** is the bucket name

The node value is not used (for now at least), so it's set to `null`
(the JSON `null` value stored as a binary string).

More details on actions taken when specific entries are processed from
the metadata log:

* when processing a log entry that is an update of bucket attributes,
  look at whether any lifecycle action is enabled for that bucket:
  * if any action is enabled, create the zookeeper node for this
    owner/bucket (ignore if already exists)
  * if no action is enabled, delete the zookeeper node for this
    owner/bucket (ignore if not found)
* when processing a log entry that is a deletion of a bucket, delete
  the zookeeper node for that owner/bucket (ignore if not found).

The resulting zookeeper list should contain a current view of all
buckets that have lifecycle enabled, up to the point where the
metadata log has been processed.

### Conductor

The conductor role is to periodically do the following (cron-style task):

* check whether there is a pending backlog of tasks still to be
  processed from Kafka, by looking at nodes under
  `/[chroot_path]/lifecycle/run/backlog-metrics`
  * if for any topic partition for both bucket and object tasks topic
    there is an existing backlog (i.e. topic latest offset (in `topic`
    node) is strictly greater than committed consumer group offset), log
    and skip this round
* get the list of buckets with potential lifecycle actions enabled
  from zookeeper (getChildren in zookeeper API)
* for each of them, do the following:
  * publish a message to the kafka topic
    *backbeat-lifecycle-bucket-tasks* (see [bucket tasks](#bucket-tasks)
    message format)

The conductor will only generate messages to *start* a new
listing. Later on, if the listing is truncated, new messages will be
generated by the Producer to create new tasks to continue the
listing. This way, we can efficiently parallelize the processing of
large buckets across producers (see [Producer](#producer) section).

Note that as an optimization, when lifecycle rules only apply to a
particular object prefix in a bucket, it can be interesting to list
only the specific range belonging to this prefix. For this we can add
the necessary info in the message JSON data to limit the listing to
this range.

### Producer

The producer's responsibility is to list objects in lifecycled
buckets, and publish to kafka queues the actual lifecycle actions to
execute on objects. For now, supported actions are expiration actions.

More specifically, the producer is part of a kafka consumer group that
processes items from the *backbeat-lifecycle-bucket-tasks* topic. For
each of them, it does the following:

* Generate or use cached credentials to be able to query the target
  bucket (through admin-backbeat.json?)
* Get the lifecycle configuration from S3 connector of the target
  bucket (GET /foobucket?lifecycle)
  * If no lifecycle configuration is associated to the bucket, skip
    the rest of bucket processing
* List the object versions in the bucket, starting from the
  *keyMarker*/*versionIdMarker* specified in the kafka entry, or from
  the beginning if not set. The limit of entries is 1000 (list
  objects hard limit), but a lower limit may be set if needed.
  * If the listing is not complete, publish a new entry to the
    *backbeat-lifecycle-bucket-tasks* topic, similar to the one
    currently processed but with the *keyMarker* and
    *versionIdMarker* fields set from what's returned by the current
    finished listing. Then another producer will keep going with the
    listing and processing of this bucket.
  * For each object listed, match it against the lifecycle rules
    logic. For each lifecycle rule matching (e.g. expiry), publish to
    the *backbeat-lifecycle-object-tasks* kafka topic an entry that
    contains the required info for the consumer to execute the
    lifecycle action (see [object tasks](#object-tasks) message format).

Note: following Amazon's behavior:

> If lifecycle configuration is enabled on the source bucket, Amazon
> S3 puts any lifecycle actions on hold until it marks the objects
> status as either COMPLETED or FAILED.

It's probably wise to apply the same logic, to guarantee we're not
processing lifecycle on an object version with a pending replication.

NOTE: This is not currently implemented.

Periodically (every minute in the current default settings) the
internal consumer of the bucket tasks topic publishes its current
committed offset and the latest topic offset in zookeeper under
`/[chroot_path]/lifecycle/run/backlog-metrics/...`, for checking in
the conductor.

### Consumer

The consumer's responsibility is to execute the individual lifecycle
actions per object that has been matched against lifecycle rules.

It's part of a kafka consumer group that processes items from the
*backbeat-lifecycle-object-tasks* topic. For each of them, it does the
following:

* Generate or use cached credentials to be able to execute actions on
  the target object (through admin-backbeat.json?)
* Check the "action" field, and execute the corresponding action:
  * for "deleteObject" action:
    * do a HEAD object with a "If-Unmodified-Since" condition on the
      last-modified date stored in the "details" of the kafka entry
    * if the request succeeds, do a DELETE request on the object
    * NOTE: this has an intrisic race, we'll be working on
      implementing the If-Unmodified-Since directly on the DELETE
      operation to get rid of this race.
  * We may consider adding more types of operations in the future

Periodically (every minute in the current default settings) the
internal consumer of the object tasks topic publishes its current
committed offset and the latest topic offset in zookeeper under
`/[chroot_path]/lifecycle/run/backlog-metrics/...`, for checking in
the conductor.

### Message formats

#### Bucket tasks

A message in *backbeat-lifecycle-bucket-tasks* topic represents a
listing task for one bucket, it has the following format:

```
{
    "action": "processObjects",
    "target": {
        "owner": "ownerID",
        "bucket": "bucketname"
    },
    "details": {
        ["prefix": "someprefix"]
        ["keyMarker": "somekeymarker"]
        ["versionIdMarker": "someversionidmarker"]
        ["uploadIdMarker": "someuploadidmarker"]
        ["marker": "somemarker"]
    }
}
```

* **prefix** may be set to limit the listing to a prefix
* **keyMarker** and **versionIdMarker** are set when resuming a
   listing from where it ended in a previous listing task.

#### Object tasks

The message format for the *backbeat-lifecycle-object-tasks* kafka
topic is the following:

```
{
    "action": "actionname",
    "target": {
        "owner": "ownerID",
        "bucket": "bucketname",
        "key": "objectkey",
        "version": "objectversion"
    }
    "details": {
        "lastModified": "..."
        [...]
    }
}
```

* **action**: type of lifecycle action, e.g. "deleteObject"
* **details**: additional info related to the action

## LINKS

* AWS Lifecycle Reference:
  https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html
