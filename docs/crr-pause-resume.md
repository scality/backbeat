# Cross-Region Replication (CRR) Manual Pause and Resume

## Description

This feature offers a way for users to manually pause and resume cross-region
replication (CRR) operations by storage locations.

## Design

A RESTful API will expose methods for users to pause and resume cross-region
replication operations.

Redis offers a pub/sub function. We will utilize this function to propagate
requests to all active CRR Kafka Consumers on all nodes with backbeat
containers setup for replication.

We want to pause and resume the CRR service at the lowest level (in our case,
pause and resume all Kafka Consumers subscribed to the CRR topic). We want to
perform these actions at the lowest level in order to stop processing any
replication entries that might have already been populated by Kafka but have yet
to be consumed and queued for replication. Any entries that have already been
consumed by the Kafka Consumer and are being processed for replication will
continue to finish replication and will not be paused.

The API will have a Redis instance publishing messages to a specific channel.
All CRR Kafka Consumers will subscribe to this channel and complete any given
valid requests.

When a Kafka Consumer pauses, the Consumer is still kept alive and maintains
any internal state, including offset. The Consumer will no longer be
subscribed to the CRR topic, so will no longer try consuming any entries.
When the paused Consumer is resumed, it will again resume consuming entries
from its last offset.

## Definition of API

* POST `/_/crr/pause`

    This POST request is to manually pause the cross-region replication service
    for all locations configured as destination replication endpoints.

    Response:
    ```sh
    {}
    ```

* POST `/_/crr/pause/<location-name>`

    This POST request is to manually pause the cross-region replication service
    for a specified location configured as a destination replication endpoint.

    Response:
    ```sh
    {}
    ```

* POST `/_/crr/resume`

    This POST request is to manually resume the cross-region replication
    service for all locations configured as destination replication endpoints.

    Response:
    ```sh
    {}
    ```

* POST `/_/crr/resume/<location-name>`

    This POST request is to manually resume the cross-region replication
    service for a specified location configured as a destination replication
    endpoint.

    Response:
    ```sh
    {}
    ```
