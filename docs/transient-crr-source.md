# Transient CRR Source

## Description

The goal of this enhancement is to avoid the cost of reading data from
a public cloud one or more times for CRR purpose, when both the
primary and CRR targets are on public clouds.

It may also be used for low-latency writes to local storage before
having CRR transitioning the data asynchronously to a cloud target.

## Requirements

It must be possible to configure object locations and CRR targets to
store objects on multiple public clouds (aka. external targets)
without a permanent local storage target, while being able to read
locally for CRR purpose, avoiding the cost of reading from public
clouds for CRR.

## Design

### Design overview

CRR process reads from the source defined by the location constraint,
writes to one or more destinations asynchronously and updates statuses
to *COMPLETED* on the source when done.

We are going to keep this core behavior, but introduce the notion of
*transient source* as a temporary primary location for a new object,
until the CRR completes the processing of this object.

The following will then happen when an object is put to a location
which is configured as being a transient source:

* the object will first be put to the location which is configured as
  being a transient source, then the put status is returned as usual,

* CRR processors will transfer the object asynchronously to all CRR
  targets set through replication API (also as usual),

* finally, once all CRR targets have been processed, the object
  location will be altered to be the preferred (default) CRR target
  location, and the transient data will be deleted asynchronously by a
  garbage collection process.

If for some reason the CRR cannot be completed to some backends, the
data will be held on the transient source, and the data will still be
readable from both the transient source natively and from the
successful CRR targets, either requesting directly the public cloud or
through a CloudServer GET specifying the preferred location in a
header. Once CRR is eventually completed successfully to all backends
(either through automatic or manually-triggered retries), the
transient source data will be removed.

### Detailed design

Here are the changes per component:

#### Cloudserver

Cloudserver is updated to support the new `isTransient` attribute of
location constraints:

* When receiving a PUT, if the location constraint that applies has a
  `isTransient` config attribute set to `true`, it will specify in the
  replication info of the object the one CRR location that is the
  future non-transient location of the object.

#### Triggers in kafka messages

We introduce triggers as an extension of kafka messages exchanged
during backbeat processing.

Triggers are a generalized way to tell processors that something has
to be done after the target action is complete, or after some specific
condition is fullfilled. Only the actions and conditions necessary for
transient source support are implemented, but it can be extended for
future use.

Triggers contain two parts:

* action: what to do when the trigger is activated

* condition: which state must be obtained in order to activate the
  trigger

For transient source support, we implement the following:

* actions
  * **transition**: switch data location, then GC old data location on
  success

* conditions
  * **replicationCompleted**: all CRR status are *COMPLETED*

For now, triggers are produced by CRR processors, and are consumed
only by the replication status processor. They could be published and
consumed by other processes in the future.

##### Message format

We are changing the message format for the replication status
processor, because:

* it allows extensibility, where the current format does not (it's
  basically a raw copy of object metadata)

* it will reduce significantly the message size, because it's not
  necessary to include the whole metadata as only a small set of
  fields are needed

Example format of a new format CRR status message containing a
location trigger:

```json
{
    "action": "updateReplicationSiteStatus",
    "target": {
        "owner": "ownerID",
        "bucket": "bucketname",
        "key": "objectkey",
        "version": "objectversion"
    },
    "details": {
        "site": "awsbackend",
        "dataStoreVersionId": "AWS-42abc",
        "replicationSiteStatus": "COMPLETED"
    },
    "triggers": [
        {
            "on": "replicationCompleted",
            "do": {
                "action": "transitionLocation",
                "target": {
                    "owner": "ownerID",
                    "bucket": "bucketname",
                    "key": "objectkey",
                    "version": "objectversion"
                },
                "details": {
                    "site": "azurebackend"
                }
            }
        }
    ]
}
```

#### Backbeat

##### CRR replication queue processor

The replication queue processor knows when one CRR location is going
to be the default location for the object after CRR is complete, by
looking at the replication info extended by cloudserver for transient
source support.

Once the CRR target is written, the processor publishes to the
backbeat-replication-status topic as usual, but it adds a trigger to
the messages with a "transition" action and a "replicationCompleted"
condition.

The format of the published messages changes in two ways:

* We're no longer publishing the whole object's metadata, but only the
  info necessary to locate the object and do the necessary action, so
  the S3 request params etc.

* We add a new "triggers" attribute containing triggers information.

##### CRR replication status processor

Two main changes on the CRR status processor:

* It handles a new kind of kafka messages that do not contain the full
  object metadata anymore, but only the info necessary to locate the
  object and do the necessary action.

* Its role is extended to apply triggers bound to messages in the new
  format.

TBD: keep backward compatibility with the old format (is it needed?) -
It will not be possible to attach triggers to the old format though.

For now it implements the "transition" action, and checks the
"replicationCompleted" condition.

The transition action asks for changing the location of the object to
a particular location (the trigger details contain info about the new
target location).

The transition action also publishes a message to the `backbeat-gc`
queue to delete the old data location on success to write the source
metadata.

##### Garbage collector

We introduce a new garbage collector process, which is a kafka
consumer of the garbage collection queue (`backbeat-gc`). It will get
rid of the data locations defined in the messages using a new backbeat
route for batch deletes.

Eventually this process could be used to garbage-collect data from
other processes (e.g. for MPU).

#### Zenko deployment with Kubernetes

TBD

## Contract provided

* Data will use space on the transient source no more time than needed
  to sync the data to all CRR targets.

* After CRR is complete, the object location should be updated to
  point to the preferred (default) CRR target.

## Useful Explanations

## Dependencies

The feature relies on Backbeat. This means that backbeat must be
enabled to use transient source (as it should be anyways by default).

## Operational Considerations

### Link with CRR

This feature only works when buckets are configured with at least one
replication target.

In case an object is put in a bucket not configured with CRR, it will
stay on the transient source until deleted by the user, like if it was
a non-transient location.

### Storage limit

There is a risk to fill up the transient source, in case the backlog
of CRR to the clouds is too important. This may occur in the following
cases:

* the rate at which data is ingested is greater than the rate at which
  objects can be synced to all CRR targets (slow network etc.)

* one of the target clouds is not available for a period of time long
  enough to let the incoming data fill up the transient source

To tackle this issue, we'll introduce a storage limit on the transient
source used storage space as a whole, above which new writes will be
refused until the transient source used space diminishes below a
certain threshold. This will be done as a separate feature though.

## Rejected Options

### Cache layer

There were discussions about implementing a caching layer on top of
S3, where all incoming data would be duplicated for a certain period
of time. This option has been rejected because of its complexity and
the short time frame available, and because we found a satisfactory
simpler alternative.
