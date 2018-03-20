# Generic Metadata Ingestion to MongoDB

## Description

This is the design document discussing the ingestion process of existing metadata
into Zenko with MongoDB.

The primary use case is to copy the existing metadata from the RING and S3 Connector
and ingest into the MongoDB backend that is used with Zenko; future use cases will
include copying metadata from existing storage solutions on AWS or Azure to MongoDB.

This specific development will allow Zenko instances to copy and ingest existing
metadata,
copying the information from the raft logs completely and allowing MongoDB to be
a parallel metadata database.

## Purpose

* Copy all existing metadata from pre-existing storage solutions to MongoDB in Zenko.

* Allow MongoDB to be used in parallel with the existing metadata servers with S3
  Connector, so that the primary metadata backend can be changed between the metadata
  servers and the MongoDB backend.

## Design

The proposed design will be as follows:

* First, determine if this is a fresh install of Zenko or an upgrade installation.
    * This can be determined by checking if Zookeeper has stored a sequence ID.
* If this is a fresh install of Zenko, we will create a `snapshot`.
    * Record the last sequence ID from the Metadata server (send a query using the
      `metadataWrapper`) that is part of the S3 connector - this will serve as a
      marker that will be stored as a `log offset` to bookmark the point between
      logs that existed prior to the start of the Zenko instance, and the point
      where new logs are added during the process where the old logs are copied.
    * From backbeat, we will use the S3 protocols to send the following requests
      to the S3 connector:
        * List all buckets that exist on the storage backend.
        * For each bucket, list all objects in the buckets. This will return info
          such as the `object key`, and if `versioning` is enabled, `version id`.
      Using the list of object keys, send a query directly to the metadata server
      with the `metadataWrapper` class in Arsenal.
        * This will get the JSON object for each object, which is put into kafka.
* If the instance of Zenko is a fresh install or the `snapshot`process has completed,
  resume queueing from raft logs using the last stored `sequence id` as the new
  starting point.

With this design, Backbeat will include a new populator which reads from the Metadata
server and queues the logs to Kafka. The consumer will not need to be changed.

## Dependencies

* MongoDB
* Existing S3 Connector
* Backbeat (including Backbeat dependencies, e.g. Zookeeper and Kafka)

## Considerations

While taking the `snapshot` of the S3 Connector/RING, we will have to pause the
consumer that will be writing to Mongo DB. This will be done via supervisord.

## Background Information for Design

* The `sequence ID` from Metadata Server will be in numerical order.
* There will not be data that is put directly into the Zenko cloudserver - all
  data will be sent to the S3 Connector, and logs will come from Metadata Server.
* There will not be pre-existing data in the Mongo database.

## Rejected Options

* One of the designs proposed was to replicate and ingest data from buckets, one
  bucket at a time. This would allow the user to customize which buckets to copy
  metadata to MongoDB. This could cause some issues:

  * The metadata servers and the MongoDB backend will have to constantly communicate
    and keep track of which buckets have been replicated between one another.

  * We will have to come up with an efficient way of filtering logs, which will be
    more time consuming than simply using the filter that is built-in with MongoDB.

## Diagram of work flow, to be updated

```
                    +-------------+
                    |             |
                    | Cloudserver |
                    |             |
                    +------+------+   Use S3 calls to
                           |          list buckets and  +----------------------+
                           |          objects that are  |                      |
                           |          in the pre-       |     S3 Connector     |
                           |          existing backend  |                      |
                           |        +-----------------> | +------------------+ |
                           |        |                   | |                  | |
                           v        | ----------------> | |  Metadata Server | |
                  +-----------------+  Obtain list of   | |                  | |
                  | +-------------+ |  object keys      | +--------^---------+ |
                  | |             | |                   +----------|-----------+
+----------+      | |  Kafka      | |                              |
|          | <------+             | |                              |
| Mongo DB |      | +-------------+ |                              |
|          |      | |             | +------------------------------+
+----------+   +-----+ Zookeeper  | |  Using the list of object keys,
               |  | |             | |  call the Metadata Server directly
               |  | +-------------+ |  to get JSON of each object
               |  |                 |
               |  |    Backbeat     |
               |  |                 |
               |  +-----------------+
               |
               |     On startup, check Zookeeper to
               +---> see if a 'sequence id' already
                        exists.

```
