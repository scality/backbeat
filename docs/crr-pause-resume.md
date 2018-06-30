# Cross-Region Replication (CRR) Manual Pause and Resume

## Description

This feature offers a way for users to manually pause and resume cross-region
replication (CRR) operations by storage locations.

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

* POST `/_/crr/resume`

    This POST request is to manually resume the cross-region replication
    service for all locations configured as destination replication endpoints.

    Response:
    ```json
    {}
    ```

* POST `/_/crr/resume/<location-name>`

    This POST request is to manually resume the cross-region replication
    service for a specified location configured as a destination replication
    endpoint.

    Response:
    ```json
    {}
    ```
