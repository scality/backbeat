# *Generic Metadata Ingestion*

## Description

This is the design document for ingesting existing metadata into MongoDB.

The current (primary) use case is to ingest existing metadata from the RING and
S3 Connector; future use cases will include ingesting metadata from existing data
on AWS or Azure.

This specific development will allow Zenko instances to ingest existing metadata,
moving the information from the raft logs completely and allowing MongoDB to be
the sole metadata database.

## Requirements

* Move all existing metadata from pre-existing storage solutions to MongoDB in Zenko.

* Replace the metadata servers with MongoDB.

## Design

*Note: current design focuses primarily on RING and Zenko*
Approach 1:

* Connect Zenko to the S3 Connector, and Backbeat will connect to each raft session
  and store a `log offset` (leading to total of 8 log offsets from raft log sessions
  0 through 7).
* Backbeat temporarily pauses real-time ingestion from the logs, and calls a listing
  on existing items in each bucket.
* Each object is then listed to get the metadata, which will be properly formatted
  and sent to the queue populator in backbeat.
* The queue populator passes each 'log' to the queue processor. The queue processor
  will:
  * Store the data in MongoDB
  * Determine if the data needs to be replicated based on existing replication streams
    * if yes, the data will be replicated from source to destination.

* Once all the information up until the `log offset` from the existing raft logs
  are formatted and processed, the new logs are parsed and added and continue to
  be updated in real time; remove/shutdown the metadata servers.

Approach 2:

* Find the `log offset` in the raft logs, ingest the existing logs up until that
  point.
* Upon startup, connect directly to MongoDB as well * so that the real-time operations
  are recorded directly into MongoDB.
* Ingest the existing logs as well, up until `log offset`.
* Since MongoDB was being populated in real time with the most recent logs, we will
  not have to continue reading from the `log offset`.
* Once the ingestion of existing logs is complete, remove/shutdown the metadata servers.

## Dependencies

* MongoDB
* Existing data/metadata
* Backbeat

## Operational Considerations

To be determined.

## Rejected Options

* One of the designs proposed was to replicate and ingest data from buckets, one
  bucket at a time. This would allow the user to customize which buckets to move
  to mongodb and replicate. This could cause some issues:
  * Will require continued maintenance and support on both the old metadata servers
    and new MongoDB backend, which defeats the purpose of migrating to and integrating
    MongoDB.
  * We will have to come up with an efficient way of filtering logs, which will be
    more time consuming that simply using the filter that is built in with MongoDB
