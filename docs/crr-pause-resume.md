# Cross-Region Replication (CRR) Pause and Resume

## Description

This feature offers a way for users to manually pause and resume cross-region
replication (CRR) operations by storage locations.

Users may also choose to resume CRR operations for a given storage location by a
specified number of hours from the current time. This is particularly useful
when the user knows a destination location will be down for a certain time and
wants to schedule a time to resume CRR.

## Design

The RESTful API exposes methods for users to pause and resume cross-region
replication operations.

Redis’s pub/sub function propagates requests to all active CRR Kafka Consumers
on all nodes that have Backbeat containers set up for replication.

Backbeat's design allows pausing and resuming the CRR service at the lowest
level (pause and resume all Kafka Consumers subscribed to the CRR topic) to
stop processing any replication entries that might have already been populated
by Kafka but have yet to be consumed and queued for replication. Any entries
already consumed by the Kafka Consumer and being processed for replication
finish replication and are not paused.

The API has a Redis instance publishing messages to a specific channel. Queue
processors subscribe to this channel, and on receiving a request to pause or
resume CRR, notify all their Backbeat consumers to perform the action, if
applicable. If an action occurs, the queue processor receives an update on the
current status of each consumer. Based on the global status of a location, the
status is updated in ZooKeeper if a change has occurred.

When a consumer pauses, the consumer process is kept alive and maintains any
internal state, including offset. The consumer is no longer subscribed to the
CRR topic, so no longer tries to consume any entries. When the paused consumer
is resumed, it again resumes consuming entries from its last offset.

## Definition of API

* GET `/_/backbeat/api/crr/status`

    This GET request checks if cross-region replication is enabled or not for
    all locations configured as destination replication endpoints.

    Response:
    ```json
    {
        "location1": "disabled",
        "location2": "enabled"
    }
    ```

* GET `/_/backbeat/api/crr/status/<location-name>`

    This GET request checks if cross-region replication is enabled or not for
    a specified location configured as a destination replication endpoint.

    Response:
    ```json
    {
        "<location-name>": "enabled"
    }
    ```

* POST `/_/backbeat/api/crr/pause`

    This POST request is to manually pause the cross-region replication service
    for all locations configured as destination replication endpoints.

    Please note a manual pause will cause any active scheduled resumes for given
    locations to be cancelled.

    Response:
    ```json
    {}
    ```

* POST `/_/backbeat/api/crr/pause/<location-name>`

    This POST request is to manually pause the cross-region replication service
    for a specified location configured as a destination replication endpoint.

    Please note a manual pause will cause any active scheduled resumes for given
    locations to be cancelled.

    Response:
    ```json
    {}
    ```

* GET `/_/backbeat/api/crr/resume/<location-name>`

    This GET request checks if the given location has a scheduled cross-region
    replication resume job. If a resume job is scheduled, you will see the
    expected date when the resume occurs.

    You may specify "all" as the `<location-name>` to get all scheduled resume
    jobs, if any.

    Response:
    ```json
    {
        "location1": "2018-06-28T05:40:20.600Z",
        "location2": "none"
    }
    ```

* POST `/_/backbeat/api/crr/resume`

    This POST request is to manually resume the cross-region replication
    service for all locations configured as destination replication endpoints.

    Please note a manual resume will cause any active scheduled resumes for
    given locations to be cancelled.

    Response:
    ```json
    {}
    ```

* POST `/_/backbeat/api/crr/resume/<location-name>`

    This is a POST request to resume cross-region replication to a specified
    location configured as a destination replication endpoint.

    Please note a manual resume will cause any active scheduled resumes for
    given locations to be cancelled.

    Response:
    ```json
    {}
    ```

* POST `/_/backbeat/api/crr/resume/<location-name>/schedule`

    This is a POST request to schedule resuming cross-region replication
    to a specified location configured as a destination replication endpoint.
    Specify "all" as a location name to schedule a resume for all available
    destinations.

    Providing a POST request body object with an `hours` key and a valid
    integer value schedules a resume to occur in the given number of hours.

    If no request body is provided for this route, a default of 6 hours is
    applied.

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

* DELETE `/_/backbeat/api/crr/resume/<location-name>/schedule`

    This is a DELETE request to remove a scheduled resume for cross-region
    replication to a specified location configured as a destination replication
    endpoint.
    Specify "all" as a location name to make this request to all available
    destinations.

    Response:
    ```json
    {}
    ```
