# Pause and Resume

## Description

This feature offers a way for users to manually pause and resume a service
extension by storage location.

In addition, a user may choose to resume service operations for a given storage
location by a specified number of hours from the current time. This is useful
for CRR in particular because if the user knows a destination location will be
down for a certain amount of time, the user can schedule for the service to
resume at time of destination location availability.

This feature is currently available for the following service extensions:

- Cross-Region Replication (CRR)
- RING Out-of-Band Updates (RING-OOB)

## Design

The RESTful API exposes methods for users to pause or resume service operations
by storage location.

The API has a Redis instance publishing messages to a specific channel.
Each service will be subscribed and listening to their relevant channels.

### Design - CRR

Redis’s pub/sub function propagate requests to active CRR Kafka Consumers
on all nodes. The published Redis message specifies the CRR service and storage
location to pause or resume.

Backbeat's design allows pausing or resuming a service at the lowest level
(pause or resume all Kafka Consumers subscribed to a given service topic) to
stop processing any Kafka entries that might have already been populated by
Kafka but have yet to be consumed and queued. Any entries already consumed by
the Kafka Consumer and being processed will continue to be completed, so you
may see a few actions taking place right after pausing.

The API has a Redis instance publishing messages to a specific channel. Queue
processors subscribe to this channel, and on receiving a request to pause or
resume, notify all their Backbeat consumers to perform the action, if
applicable. If an action occurs, the queue processor receives an update on the
current status of each consumer. Based on the global status of a location, the
status is updated in ZooKeeper if a change has occurred.

When a consumer pauses, the consumer process is kept alive and maintains any
internal state, including offset. The consumer is no longer subscribed to the
service topic, so no longer tries to consume any entries. When the paused
consumer is resumed, it again resumes consuming entries from its last offset.

### Design - RING-OOB

Redis's pub/sub function propagate requests to the Ingestion Populator class,
which handles management of fetching metadata logs from RING sources and
produces these log entries to Kafka. The published Redis message specifies the
RING-OOB service and storage location to pause or resume.

On receiving a request to pause or resume, the Ingestion Populator will manage
the state of each RING source. Management of state is saved in ZooKeeper for
persistence.

On pause, the RING-OOB service will stop fetching metadata logs from relevant
sources. The consumer process is kept alive and it is possible to see metadata
entries continue populating in Zenko's metadata storage.

## Definition of API

* GET `/_/backbeat/api/<service>/status`

    This GET request checks if the given service is enabled or not for all
    locations configured for the service.

    Route available for following services:
    - Cross-Region Replication: `crr`
    - Metadata Ingestion: `ingestion`

    Response:

    ```json
    {
        "location1": "disabled",
        "location2": "enabled"
    }
    ```

* GET `/_/backbeat/api/<service>/status/<location-name>`

    This GET request checks if the given service is enabled or not for a
    specified location configured for the service.

    Route available for following services:
    - Cross-Region Replication: `crr`
    - Metadata Ingestion: `ingestion`

    Response:

    ```json
    {
        "<location-name>": "enabled"
    }
    ```

* POST `/_/backbeat/api/<service>/pause`

    This POST request is to manually pause the given service for all locations
    configured for the service.

    Please note a manual pause will cause any active scheduled resumes for given
    locations to be cancelled.

    Route available for following services:
    - Cross-Region Replication: `crr`
    - Metadata Ingestion: `ingestion`

    Response:

    ```json
    {}
    ```

* POST `/_/backbeat/api/<service>/pause/<location-name>`

    This POST request is to manually pause the given service for a specified
    location configured for the service.

    Please note a manual pause will cause any active scheduled resumes for given
    locations to be cancelled.

    Route available for following services:
    - Cross-Region Replication: `crr`
    - Metadata Ingestion: `ingestion`

    Response:

    ```json
    {}
    ```

* GET `/_/backbeat/api/<service>/resume/<location-name>`

    This GET request checks if the given location has a scheduled resume job.
    If a resume job is scheduled, you will see the expected date when the
    resume should occur.

    You may specify "all" as the `<location-name>` to get all scheduled resume
    jobs, if any.

    Route available for following services:
    - Cross-Region Replication: `crr`
    - Metadata Ingestion: `ingestion`

    Response:

    ```json
    {
        "location1": "2018-06-28T05:40:20.600Z",
        "location2": "none"
    }
    ```

* POST `/_/backbeat/api/<service>/resume`

    This POST request is to manually resume the given service for all locations
    configured for the service.

    Please note a manual resume will cause any active scheduled resumes for
    given locations to be cancelled.

    Route available for following services:
    - Cross-Region Replication: `crr`
    - Metadata Ingestion: `ingestion`

    Response:

    ```json
    {}
    ```

* POST `/_/backbeat/api/<service>/resume/<location-name>`

    This is a POST request to resume the given service for a specified location
    configured for the service.

    Please note a manual resume will cause any active scheduled resumes for
    given locations to be cancelled.

    Route available for following services:
    - Cross-Region Replication: `crr`
    - Metadata Ingestion: `ingestion`

    Response:

    ```json
    {}
    ```

* POST `/_/backbeat/api/<service>/resume/<location-name>/schedule`

    This is a POST request to schedule resuming a specified location configured
    for the service. Specify "all" as the location name to schedule a resume
    for all locations configured for the service.

    Providing a POST request body object with an `hours` key and a valid
    integer value schedules a resume to occur in the given number of hours.

    If no request body is provided for this route, a default of 6 hours is
    applied.

    Route available for following services:
    - Cross-Region Replication: `crr`
    - Metadata Ingestion: `ingestion`

    Request Body Example:

    ```json
    {
        "hours": 6
    }
    ```

    Response:

    ```json
    {}
    ```

* DELETE `/_/backbeat/api/<service>/resume/<location-name>/schedule`

    This is a DELETE request to remove a scheduled resume for a specified
    location configured for the service. Specify "all" as the location name to
    remove any and all scheduled resumes for all locations configured for the
    service.

    Route available for following services:
    - Cross-Region Replication: `crr`
    - Metadata Ingestion: `ingestion`

    Response:

    ```json
    {}
    ```
