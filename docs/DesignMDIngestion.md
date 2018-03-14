# *Generic Metadata Ingestion*

## Description

This is the design document discussing the ingestion process of existing metadata
into Zenko with MongoDB.

The current (primary) use case is to ingest existing metadata from the RING and
S3 Connector; future use cases will include ingesting metadata from existing storage
solutions
on AWS or Azure.

This specific development will allow Zenko instances to copy and ingest existing
metadata,
copying the information from the raft logs completely and allowing MongoDB to be
a parallel metadata database.

## Requirements

* Copy all existing metadata from pre-existing storage solutions to MongoDB in Zenko.

* Allow MongoDB to be used in parallel with the existing metadata servers with S3
  Connector.

## Design

*Note: current design focuses primarily on RING and Zenko*
The proposed design will be as follows:

* Connect Zenko to the S3 Connector, and Backbeat will connect to each raft session
  and store a `log offset` (leading to total of 8 log offsets from raft log sessions
  0 through 7).
* Backbeat temporarily pauses real-time ingestion from the logs, and calls a listing
  on existing items in each bucket.
* Each object is then listed to get the metadata, which will be properly formatted
  and sent to the queue populator in backbeat.
* The queue populator adds the log to MongoDB

* Once all the information up until the `log offset` from the existing raft logs
  are formatted and processed, the new logs starting from the `log offset` are parsed,
  added and continue to be updated in real time.

## Dependencies

* MongoDB
* Existing S3 Connector
* Backbeat (including Backbeat dependencies such as Zookeeper and Kafka)

## Operational Considerations

To be determined.

## Rejected Options

* One of the designs proposed was to replicate and ingest data from buckets, one
  bucket at a time. This would allow the user to customize which buckets to copy
  metadata to MongoDB. This could cause some issues:

  * The metadata servers and the MongoDB backend will have to constantly communicate
    and keep track of which buckets have been replicated between one another.

  * We will have to come up with an efficient way of filtering logs, which will be
    more time consuming that simply using the filter that is built in with MongoDB.
