# Zenko Backbeat Metrics

Zenko Backbeat exposes various metric routes which return a response with an
HTTP code.

## Response Codes

```
+----------+------------------------------------------------------------------+
| Response | Details                                                          |
+==========+==================================================================+
|   200    | OK: success                                                      |
+----------+------------------------------------------------------------------+
|   403    | AccessDenied: request IP address must be defined in              |
|          |   'conf/config.json' in field 'server.healthChecks.allowFrom'    |
+----------+------------------------------------------------------------------+
|   404    | RouteNotFound: route must be valid                               |
+----------+------------------------------------------------------------------+
|   405    | MethodNotAllowed: the HTTP verb must be a GET                    |
+----------+------------------------------------------------------------------+
|   500    | InternalError: this could be caused by one of several            |
|          |   components: the api server, Kafka, Zookeeper, Redis, or one    |
|          |   of the Producers for a topic topic                             |
+----------+------------------------------------------------------------------+
```

## Routes

Routes are organized in the following fashion:
`/_/metrics/<extension-type>/<location-name/[<metric-type>]/[<bucket>]/[<key>]?[versionId=<version-id>]`

Where:

- `<extension-type>` can currently only support `crr` for replication metrics
- `<location-name>` represents any current destination replication locations you
  have defined. If you would like metrics displayed for all locations, use `all`
- `<metric-type>` is an optional field. Leaving this out will get all metrics
  available for given extension and location. If you specify a metric type, you will
  get the metric specified.
- `<bucket>` is an optional field. If provided, it is the name of the bucket in
  which the object exists.
- `<key>` is an optional field. If provided, it is the object's key when getting
  CRR metrics for a particular object.
- `<version-id>` is an optional field. If provided, it is the object's version
  ID when getting CRR metrics for a particular object.

### `/_/metrics/crr/<location-name>`

This route gathers all the metrics below and returns as one JSON object for the
specified extension type and locations name.

### `/_/metrics/crr/<location-name>/backlog`

This route returns the replication backlog in number of objects and number of
total bytes for the specified extension type and location name. Replication
backlog represents the objects that have been queued up to be replicated to
another location, but the replication task has not been completed yet for that
object. If replication failed for an object, the failed objects metrics are
considered backlog.

**Example Output**:

```
"backlog":{
    "description":"Number of incomplete replication operations (count) and
    number of incomplete bytes transferred (size)",
    "results":{
        "count":4,
        "size":"6.12"
    }
}
```

### `/_/metrics/crr/<location-name>/completions`

This route returns the replication completions in number of objects and number
of total bytes transferred for the specified extension type and location.
Completions are only collected up to an `EXPIRY` time, which is currently set
to **24 hours**.

**Example Output**:

```
"completions":{
    "description":"Number of completed replication operations (count) and number
    of bytes transferred (size) in the last 86400 seconds",
    "results":{
        "count":31,
        "size":"47.04"
    }
}
```

### `/_/metrics/crr/<location-name>/failures`

This route returns the replication failures in number of objects and number
of total bytes for the specified extension type and location. Failures are
collected only up to an `EXPIRY` time, currently set to a default
**24 hours**.

**Example Output**:

```
"failures":{
    "description":"Number of failed replication operations (count) and bytes
    (size) in the last 86400 seconds",
    "results":{
        "count":"5",
        "size":"10.12"
    }
}
```

### `/_/metrics/crr/<location-name>/throughput`

This route returns the current throughput in number of completed operations per
second (or number of objects replicating per second) and number of total bytes
completing per second for the specified type and location name.

Note throughput is averaged over the past 15 minutes of data collected so this
metric is really an average throughput.

**Example Output**:

```
"throughput":{
    "description":"Current throughput for replication operations in ops/sec
    (count) and bytes/sec (size)",
    "results":{
        "count":"0.00",
        "size":"0.00"
    }
}
```

### `/_/metrics/crr/<site-name>/progress/<bucket>/<key>?versionId=<version-id>`

This route returns replication progress in bytes transferred for the specified
object.

**Example Output**:

```
{
    "description": "Number of bytes to be replicated (pending), number of bytes
    transferred to the destination (completed), and percentage of the object
    that has completed replication (progress)",
    "pending": 1000000,
    "completed": 3000000,
    "progress": "75%"
}
```

### `/_/metrics/crr/<site-name>/throughput/<bucket>/<key>?versionId=<version-id>`

This route returns the throughput in number of total bytes completing per second
for the specified object.

**Example Output**:

```
{
    "description": "Current throughput for object replication in bytes/sec
    (throughput)",
    "throughput": "0.00"
}
```

## Design

For basic metrics, 6 data points are collected:

- number of operations (ops)
- number of completed operations (opsdone)
- number of failed operations (opsfail)
- number of bytes (bytes)
- number of completed bytes (bytesdone)
- number of failed bytes (bytesfail)

In order to collect metrics, a separate Kafka Producer and Consumer
(`MetricsProducer` and `MetricsConsumer`, respective) using its own Kafka topic
(default to "backbeat-metrics") will produce its own Kafka entries.

When a new CRR entry is sent to Kafka, a Kafka entry to the metrics topic will
be produced indicating to increase `ops` and `bytes`. On consumption of this
metrics entry, Redis keys will be generated with the following schema:

Site-level CRR metrics Redis key:
`<site-name>:<default-metrics-key>:<ops-or-bytes>:<normalized-timestamp>`

Object-level CRR metrics Redis key:
`<site-name>:<bucket-name>:<key-name>:<version-id>:<default-metrics-key>:<ops-or-bytes>:<normalized-timestamp>`

Normalized timestamp is used to determine in which time interval to set the data
on. The default metrics key will end with the type of data point it represents.

When the CRR entry is consumed from Kafka, processed, and the metadata for
replication status is updated to a completed state (i.e. COMPLETED, FAILED),
a Kafka entry is sent to the metrics topic indicating to increase `opsdone` and
`bytesdone` if replication was successful or `opsfail` and `bytesfail` if
replication was not successful. Again, on consumption of this metrics entry,
Redis keys will be generated for their respective data points.

It is important to note that a `MetricsProducer` is initialized and producing
to the metrics topic both when the CRR topic `BackbeatProducer` produces and
sends a Kafka entry, and when the CRR topic `BackbeatConsumer` consumes and
processes its respective Kafka entries. The `MetricsConsumer` will process these
Kafka metrics entries and produce to Redis.

A single-location CRR entry produces up to six keys in total. The data points
stored in Redis are saved in intervals (default of 5 minutes) and are available
up to an expiry time (default of 24 hours).

An object CRR entry creates one key. An initial key is set when the CRR
operation begins, storing the total size of the object to be replicated. Then,
for each part of the object that is transferred to the destination, another key
is set (or incremented if a key already exists for the current timestamp) to
reflect the number of bytes that have completed replication. The data points
stored in Redis are saved in intervals (default of 5 minutes) and are available
up to an expiry time (default of 24 hours).

Throughput for object CRR entries are available up to an expiry time (default of
15 minutes). Object CRR throughput is the average bytes transferred per second
within the latest 15 minutes.

A `BackbeatServer` (default port 8900) and `BackbeatAPI` expose these metrics
stored in Redis by querying based on the prepended Redis keys. Using these data
points, we can calculate simple metrics like backlog, number of completions,
progress, throughput, etc.
