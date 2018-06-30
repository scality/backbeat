# Cross-Region Replication (CRR) Manual Pause and Resume

## Description

This feature offers a way for users to manually pause and resume cross-region
replication (CRR) operations by storage locations.

Users may also choose to resume CRR operations for a given storage location by a
specified number of hours from the current time. This is particularly useful
for cases where the user knows a destination location will be down for a
certain amount of time and would like to schedule a time to resume CRR.

## Design

A RESTful API will expose methods for users to pause and resume cross-region
replication operations.

Redis offers a pub/sub function. We will utilize this function to propagate
requests to all active CRR Kafka Consumers on all nodes that have backbeat
containers setup for replication.

We want to pause and resume the CRR service at the lowest level (in our case,
pause and resume all Kafka Consumers subscribed to the CRR topic). We want to
perform these actions at the lowest level in order to stop processing any
replication entries that might have already been populated by Kafka but have yet
to be consumed and queued for replication. Any entries that have already been
consumed by the Kafka Consumer and are being processed for replication will
continue to finish replication and will not be paused.

The API will have a Redis instance publishing messages to a specific channel.
QueueProcessors will subscribe to this channel, and on receiving a request
to pause or resume CRR, will notify all of their BackbeatConsumers to perform
the action if applicable. If an action occurred, the QueueProcessor will receive
an update on the current status of each Consumer. Based on the global status of
a location, the status will be updated in Zookeeper if a change has occurred.

It is important to note, when a Consumer pauses, the Consumer process is still
kept alive and maintains any internal state, including offset. The Consumer will
no longer be subscribed to the CRR topic, so will no longer try consuming any
entries. When the paused Consumer is resumed, it will again resume consuming
entries from its last offset.

## Definition of API

* GET `/_/crr/status`

    This GET request checks if cross-region replication is enabled or not for
    all locations configured as destination replication endpoints.

    Response:
    ```json
    {
        "location1": "disabled",
        "location2": "enabled"
    }
    ```

* GET `/_/crr/status/<location-name>`

    This GET request checks if cross-region replication is enabled or not for
    a specified location configured as a destination replication endpoint.

    Response:
    ```json
    {
        "<location-name>": "enabled"
    }
    ```

* POST `/_/crr/pause`

    This POST request is to manually pause the cross-region replication service
    for all locations configured as destination replication endpoints.

    Response:
    ```json
    {}
    ```

* POST `/_/crr/pause/<location-name>`

    This POST request is to manually pause the cross-region replication service
    for a specified location configured as a destination replication endpoint.

    Response:
    ```json
    {}
    ```

* GET `/_/crr/resume/<location-name>`

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

* POST `/_/crr/resume`

    This POST request is to manually resume the cross-region replication
    service for all locations configured as destination replication endpoints.

    Response:
    ```json
    {}
    ```

* POST `/_/crr/resume/<location-name>`

    This POST request is to manually resume the cross-region replication service
    for a specified location configured as a destination replication endpoint.

    Response:
    ```json
    {}
    ```

* POST `/_/crr/resume/<location-name>/schedule`

    This POST request is to schedule resuming the cross-region replication
    service for a specified location configured as a destination replication
    endpoint. You may specify "all" as a location name in order to schedule
    a resume for all available destination locations.

    Providing a POST request body object with an `hours` key and a valid
    integer value will schedule a resume to occur by given hours.

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
