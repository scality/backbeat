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

Normal CRR process reads from the source defined by the location
constraint, writes to one or more destinations asynchronously and
updates statuses to *COMPLETED* on the source when done.

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
readable from both the transient source natively or from the
successful CRR targets, either requesting directly the public cloud or
through a CloudServer GET specifying the preferred location in a
header. Once CRR is eventually completed successfully to all backends
(either through automatic or manually-triggered retries), the
transient source data will be removed.

### Detailed design

Here are the changes per component:

#### Backbeat

##### CRR replication status processor

Once the status is globally *COMPLETED* for an object (i.e. all CRR
targets are up-to-date), if the object location is transient, before
writing the source metadata through the backbeat route, the
replication status processor also updates the location of the object
to be the default configured CRR target location, if it is one of the
object's CRR targets, otherwise behavior TBD.

If the update succeeds, it then publishes a message to the
`backbeat-gc` queue to delete the transient data. The messages
should contain a list of data locations to delete, in a similar format
as the original object's locations.

##### Garbage collector

We introduce a new garbage collector process, which is a kafka
consumer of the garbage collection queue (`backbeat-gc`). It will get
rid of the data locations defined in the messages. It may use
sproxyd's batch delete API when deleting on RING for more efficiency.

Eventually this process could be used to garbage-collect data from
other processes (e.g. for MPU).

#### Federation

Note: non-zenko only

For Federation deployments, we translate the `is_transient` attributes
of location constraints into a config parameter `isTransient` in the
cloudserver config files.

#### Cloudserver

Cloudserver is updated to support the new `isTransient` attribute of
location constraints:

* When receiving a PUT, if the location constraint that applies has a
  `isTransient` config attribute set to `true`, it will set a property
  `isTransient: true` in the `location` field of object's metadata (or
  alternatively in another place in metadata in case `location` is not
  a good fit), so to inform CRR that this location is meant to be
  temporary.

## Definition of API and its parameters

Note: Federation deployment only (not valid for Zenko/K8s deployment)

Location constraints have the optional extra boolean attribute
`is_transient` that tells if this location is meant to be short-lived
and turned into the CRR location configured as the default location,
once all CRR targets are up-to-date.

Example of setting in group_vars/all:

```yaml
location_constraints:
  cloud-1:
    type: azure
  cloud-2:
    type: aws_s3
  local-ring:
    type: scality
    is_transient: true
    connector:
      sproxyd:
        chord_cos: 3
        bootstrap_list:
          - node1:4244
          - node2:4244
          - node3:4244
          - node4:4244
          - node5:4244
          - node6:4244
```

## Contract provided

* Data will use space on the transient source no more time than needed
  to sync the data to all CRR targets.

* After CRR is complete, the object location should be updated to
  point to the preferred (default) CRR target.

## Useful Explanations

## Dependencies

The feature relies on Backbeat CRR.

This means that backbeat must be enabled to use transient source.

## Operational Considerations

### Link with CRR

This feature only works when buckets are configured with at least one
replication target.

In case an object is put in a bucket not configured with CRR, it will
stay on the transient source until deleted by the user.

### Quota

There is a risk to fill up the transient source, in case the backlog
of CRR to the clouds is too important. This may occur in the following
cases:

* the rate at which data is ingested is greater than the rate at which
  objects can be synced to all CRR targets (slow network etc.)

* one of the target clouds is not available for a period of time long
  enough to let the incoming data fill up the transient source

To tackle this issue, we'll introduce a quota limit on the transient
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
