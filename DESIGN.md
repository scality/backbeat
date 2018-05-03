# Considerations

## Containerized

Resides in its own container.

## Distributed

Multiple instances per site for high availability. If an instance dies,
the queue replica from another instance can be used to process records.

## Extensible

The core engine can be extended to achieve features such as asynchronous
replication.

## Backend-Agnostic

All interactions shall go through S3. This way it is not restricted to one
backend and existing solutions for different backends can be reused.

## Background Jobs

Backbeat shall be built to include background tasks that can be run on a
crontab schedule.

## Multi-Threaded

Using Kafka's KeyedMessage mechanism, multiple consumers can process
entries in parallel, achieving high throughput.

# Design Overview

![design](/res/design-overview.png)

## Deployment (Mono/Multi-site)

The nodes of a Backbeat cluster must reside within a single data center.
Ideally, there will be one stand-alone cluster per site. To ensure high
availability, each cluster shall deploy multiple instances.

Backbeat will provide the backbone of features such as asynchronous
replication, which rely on local services to perform remote actions.
The basis of replication relies on versioning, metadata, and data
propagation from site to site, which does not require sharing the
message queue between sites (also undesirable because Backbeat's
internals are not necessarily geo-aware).

## Kafka

Kafka is Backbeat's queue manager.

### Pros

* Distributed, Pub/Sub, simple to use, durable replicated log.
* Fast reads and writes using sequential reads/writes to disk.
* Topics contain partitions that are strictly ordered.
* Replication is done at a partition level and can be controlled
  per-partition.
* More than one consumer can process a partition, achieving parallel
  processing.
* Supports one consumer processing in real time and another processing
  in batch.
* A large number of consumers can easily be added without affecting
  performance.
* Stream processing can be leveraged for encryption, compression, etc.

### Cons

* Uses ZooKeeper but it’s not in the critical path
* Requires optimization for fault tolerance
* Not designed for high-latency environments

# Applying Backbeat

## Using Backbeat for Asynchronous Replication

![crr](res/backbeat-crr.png)

For asynchronous replication, Backbeat can replicate objects from a
bucket in one region to another. The design highlights how this
feature can be achieved by using Backbeat.
See AWS's rules for [What Is and Is Not Replicated](http://docs.aws.amazon.com/AmazonS3/latest/dev/crr-what-is-isnot-replicated.html).

**Note:** For Active/Passive setups, the target is READONLY. In a
disaster-recovery setup, on losing Site A, a customer can point to
Site B for READONLY actions until Site A is back up. Although the
customer can write to Site B, these writes are not replicated to
Site A (among other reasons because Site A may have data that is
not fully replicated to Site B when the failure occurred).

* All replication actions go through S3. S3 exposes routes for
    metadata and data backends to Backbeat.

* The Metadata journal is the source of truth.

* Object metadata must have a new property, `replicationStatus`. When a CRR
    configuration is enabled on a bucket, any S3's PUT metadata operations on
    objects matching the configuration will have `replicationStatus` as
    `PENDING`.

* If a producer (background job) wakes up every `n` seconds and requests
    a journal from Metadata starting from a sequenceId, the producer
    queues entries from the journal that have `replicationStatus` PENDING
    to Kafka.

* The entries contain the sequence number as key and a stringified JSON
    with fields like action (PUT), object key, object metadata.

* Backbeat will leverage Kafka's KeyedMessage mechanism, guaranteeing that
    all records with the same key, for example `<bucket name>:<object key>`,
    are written to the same partition, and that record ordering is guaranteed
    within a partition. This offers the advantage balancing records across
    all Kafka nodes and assigning multiple consumers to process the queue.

* A consumer picks up from the last committed offset and processes one entry
    at a time. The consumer first copies data to the target site, updates the
    object's metadata with the new location, sets replicationStatus to
    `REPLICA` and writes the metadata entry at the target. Once the metadata
    and data operations are completed on the target bucket, the object's
    source bucket metadata is updated to reflect replicationStatus as
    `COMPLETED`.

* On any failure in this sequence of operations for an entry, the object
    metadata on the source is updated with replicationStatus `FAILED`.
    The consumer commits the offset of the last successful entry
    processed to Kafka. This entry is retried on the next run.

# Security

* **End-to-End SSL:** Use PKI certificates with TLS for trusted communication
    between servers. All sites (SF, NY, and Paris, for example),
    communicating through a private route establish a TCP connection
    using PKI certificates.

* **Temporary credentials:** Backbeat shall have no credentials (access key,
    secret key pair) by default and must acquire temporary credentials from IAM
    to perform actions on users' resources.

* **Trust Policy:** A trust policy (IAM actions) will be created, letting
    the user assume a role to gain temporary credentials to the
    source/destination account's resources.

* **ACL Account Management Permissions:** Cross-account permissions can be
    managed using ACLs. Access policies (S3 actions) will be created
    allowing the role to perform specific actions to achieve replications.
    On establishing a trusted connection using the certificates, Backbeat
    requests temporary credentials from IAM. Temporary credentials expire
    every hour (or some set time). These credentials are used to communicate
    with S3 for replications.

* **Credential Renewal:** Credentials are renewed when they expire.
    Temporary credentials don’t span across sites, so Backbeat must renew
    credentials on all sites individually.

* **AWS conformity:** The setup and behavior must conform to AWS's
    specifications for asynchronous replication. Conformity assures that
    credentials in Backbeat and roles aren't hardcoded and that policies
    are transparent to customers, who can see what actions we use for
    replication. Customers are not expected to maintain or alter these
    policies: Scality will manage them at deploy time.

# OSS Licenses

* Kafka and ZooKeeper are the only Backbeat dependencies licensed
    under Apache License 2.0.
* `node-rdkafka` npm module, used for producer/consumer actions
    and maintained by Blizzard Entertainment is MIT-licensed.

# Statistics for SLA, Metrics, etc.

There are two ways to approach this:
1. Use Pub/Sub events in addition to the MetaData log in a separate topic.
  Leverage the records in this topic by comparing to the active queue to
  generate statistics like:

  - RPO (Recovery Point Objective)
  - RTO (Recovery Time Objective)
  - Storage backlog in bytes
  - Storage replicated so far in bytes
  - Number of objects in backlog
  - Number of objects replicated so far etc.

2. Use a decoupled topic in addition to the queue topic. The
  producers/consumers will manage this by adding records for
  non-replicated and replicated entries. Because each entry has
  a sequence number, calculating the difference between sequence
  numbers of the latest non-replicated and replicated records
  can provide the required statistics.

# Writing Extensions

A Backbeat extension enables adding more functionality to the core
backbeat asynchronous processor. E.g. Asynchronous Replication is one
of the extensions available for backbeat.

Extensions are located in the extensions/ directory, with a
sub-directory named after the extension name (lowercase).

## Extending the Queue Populator

The queue populator is a core Backbeat process that reads entries
from the metadata log periodically and provides them to all running
extensions through the filter() method, while maintaining the
offsets of the latest processed log entries.

Extensions can publish messages to one or more Kafka topics on receiving
particular log entry events.

To achieve this, create an 'index.js' file in the extension module
directory, exporting the attributes defining the extension metadata
and entry points. For example, to create a "mycoolext" extension, put
the following in extensions/mycoolext/index.js:

```
module.exports = {
    name: 'my cool extension',
    version: '1.0.0',
    queuePopulatorExtension: require('./MyCoolExtensionPopulator.js'),
};
```

Here, MyCoolExtensionPopulator.js exports a class that:

- Inherits from the lib/queuePopulator/QueuePopulatorExtension class.
- Implements the filter() method, which is called with an "entry" argument
  containing the fields ``{ bucket, key, type=(put|del), value }`` whenever new
  metadata log entries are fetched.
- Calls the QueuePopulatorExtension.publish() method from the filter()
  method to schedule queueing of Kafka entries to topics.

Extensions are enabled based on config.json having a subsection in
"extensions" that matches the name of the extension directory.

For example, the following config.json passage enables the mycoolext
extension and provides configuration parameter "someparam" as "somevalue"
to the extension classes.

```
{
    [...]
    "extensions": {
        "mycoolext": {
            "someparam": "somevalue"
        }
    }
}
```
