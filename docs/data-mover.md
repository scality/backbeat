# Data Mover Service

## OVERVIEW

The data mover service can copy object data as a service.

It consumes action messages from a kafka topic ("backbeat-data-mover"
by default), and optionally returns the action outcome in another
kafka topic, which name can be set in the action message.

Each action message type defines the type of copy we want, described
below. The recommended way to pass action messages and receive results
is to use the lib/models/ActionQueueEntry class to create instances of
actions that can be manipulated, but it's not mandatory as messages
can be created and parsed directly to/from JSON format.

Attributes in parenthesis are optional.

## COPY LOCATION ACTION

The "copy location" action copies a zenko object to a new location.

### Action message format

This is the format of "copy location" actions that the data mover
expects:

```
{
    "action": "copyLocation",
    ("actionId": "uuid-xyz",)
    "target": {
        "bucket": "bucketName",
        "key": "objectKey",
        "contentMd5": "0123456789abcdef0123456789abcdef",
        ("version": "versionNumber"),
    },
    "toLocation": "zenkoLocationName",
    ("resultsTopic": "results-kafka-topic-name"),
    ("context": { /* user-defined context fields */ },)
}
```

### Action result format

This is the format of "copy location" responses that the data mover
will send to the results topic if set, with all original action
message fields preserved:

```
{
    "action": "copyLocation",
    ("actionId": "uuid-xyz",)
    "target": {
        "bucket": "bucketName",
        "key": "objectKey",
        ("version": "versionNumber")
    },
    "toLocation": "zenkoLocationName",
    ("resultsTopic": "results-kafka-topic-name",)
    ("context": { /* user-defined context fields */ },)

    "status": "success|error",
    // if status is "success":
    ("results": {
        "location": [{
            // location-specific info (key, dataStoreVersionId etc.)
        }]
    }),
    // if status is "error":
    ("error": {
        "code": xxx // original error.code, usually HTTP error code
        "message": "ErrorType" // original error.message, usually
                               // arsenal error type
        "description": "lengthy description", // original error.description
    })
}
```
