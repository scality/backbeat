# Transient CRR Source

## Description

The goal of this enhancement is to avoid the cost of reading data from
a public cloud one or more times for CRR purpose, when both the
primary and CRR targets are on public clouds.

It may also be used for low-latency writes to local storage before
having CRR transitioning the data asynchronously to a cloud target.

## Requirements

* It must be possible to configure object locations and CRR targets to
  store objects on multiple public clouds (aka. external targets)
  without a permanent local storage target, while being able to have
  backbeat read from a local, non-cloud source for CRR purpose,
  avoiding the cost of reading from public clouds.

* It must be possible to set a limit on the storage space consumed by
  a transient location, and returning errors to PUT requests if the
  limit is reached.

## Design

### Design overview

CRR process reads from the source defined by the location constraint,
writes to one or more destinations asynchronously and updates statuses
to *COMPLETED* on the source when done.

Transient source relies on this core behavior, but refers to a
temporary primary location for a new object, until the CRR completes
the processing of this object.

The following then happens when an object is put to a location which
is configured as being a transient source:

* the object is first put to the location which is configured as a
  transient source, then the put status is returned as usual,

* CRR processors transfer the object asynchronously to all CRR targets
  set through replication API. It sends an extra header in the GET
  request "x-amz-location-constraint" to ensure reads are done from
  the transient source location, not from the *preferred read
  location*,

* finally, once all CRR targets have been processed, the transient
  data is deleted asynchronously by the garbage collector backbeat
  service. Reads from users still happen on the *preferred read
  location*.

If for some reason the CRR cannot be completed to some backends, the
data will be held on the transient source, and the data will still be
read from the *preferred read location* if CRR was successful on
it. If CRR is either PENDING or FAILED to the preferred read target,
it's still possible to provide the "x-amz-location-constraint" header
on GET requests, specifying the location from which to read the data.

Once CRR has eventually completed successfully to all backends (either
through automatic or manually-triggered retries), the transient source
data is garbage-collected asynchronously, and the object will not be
readable anymore from the transient source. We do not update object
metadata to reflect the fact that the data on transient source has
effectively been deleted, the user must be aware that only the CRR
cloud targets remain valid sources.

### Detailed design

Here are the changes per component:

#### CloudServer

* support for "isTransient" property on locations: enforces that
  replication configuration of buckets that belong to transient
  locations specify a *preferred read location*

* new backbeat route `/_/backbeat/batchDelete`, that is used by the
  garbage collector process to delete data locations. It is used
  through POST requests containing a JSON body that is an array of
  locations to delete.

* modified support of *preferred read locations*: now it's set through
  the replication configuration of a bucket, by appending the
  `:preferred_read` suffix to the given location in the
  `<StorageClass>` attribute. This has support from the Orbit user
  interface through the locations setup.

* added storage limit support through the use of Redis keys storing
  the consumed storage for each location. The storage limit is set
  with the `sizeLimitGB` optional location property. When a PUT
  request comes in, it is denied with an error if the extra storage
  required for this object would go beyond the configured limit.

#### Backbeat

##### CRR replication status processor

When the global status for an object stored on a bucket whose location
is transient reaches the *COMPLETED* status, and the metadata update
is successful, the CRR replication status processor sends a message to
the **backbeat-gc** kafka queue containing the list of transient data
locations to garbage-collect.

##### Garbage collector

We introduce a new garbage collector process, which is a kafka
consumer of the garbage collection queue (**backbeat-gc**). It will get
rid of the data locations defined in the messages using a new backbeat
route for batch deletes.

Eventually this process could be used to garbage-collect data from
other processes (e.g. for MPU).

##### CRR data processor

Added a check on source metadata before starting replicating data, to
skip if the status is COMPLETED for the location the data processor is
responsible for. This is done on transient source location as well for
non-transient locations as a sanity check.

Rationale: for transient source it's most important to handle
duplicate kafka entries from the replication topic, and become
idempotent in such case, because replication from a transient source
enforces the read to happen on the local (primary) storage, which does
not work as soon as the local data has been deleted (the data
processor would eventually set the status to FAILED on top of the
COMPLETED one otherwise because of the missing data to replicate).

#### Zenko deployment with Kubernetes

* The garbage collector is a new backbeat service on its own, with its
  own chart.

* Still relies on CRR service for the rest of processing

* The "isTransient" location property is set through Orbit. Later on
  we can consider setting it through an environment variable set at
  deployment time.

## Contract provided

* Data will use space on the transient source no more time than needed
  to sync the data to all CRR targets.

* After CRR is complete, the object is readable from the *preferred
  read location* specified by the user in the replication
  configuration.

* In case the transient source receives data quicker than it can
  replicate to clouds, its consumed storage may grow. We added an
  optional storage limit property to locations, which if reached,
  prevent writes to happen, returning an error instead.

## Useful Explanations

## Dependencies

The feature relies on Backbeat. This means that backbeat must be
enabled to use transient source (as it should be anyways by default).

## Operational Considerations

### Link with CRR

This feature only works when buckets are configured with at least one
replication target. In case an object is put in a bucket not
configured with CRR, it will stay on the transient source until
deleted by the user, like if it was a non-transient location.

The replication configuration of buckets which location is transient
must have a *preferred read location* defined. If not, the replication
configuration is rejected when set with put-bucket-replication.

### Storage limit

There is a risk of filling up the transient source, in case the backlog
of CRR to the clouds is too important. This may occur in the following
cases:

* the rate at which data is ingested is greater than the rate at which
  objects can be synced to all CRR targets (slow network etc.)

* one of the target clouds is not available for a period of time long
  enough to let the incoming data fill up the transient source

To tackle this issue, the storage limit feature prevents new writes
until the transient source used space diminishes below a certain
threshold.

## Rejected Options

### Cache layer

There were discussions about implementing a caching layer on top of
S3, where all incoming data would be duplicated for a certain period
of time. This option has been rejected because of its complexity and
the short time frame available, and because we found a satisfactory
simpler alternative.
