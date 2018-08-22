# Zenko Backbeat Metrics

Zenko Backbeat exposes various metric routes that return a response with an
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

Routes are organized as follows:
`/_/backbeat/api/metrics/<extension-type>/<location-name/[<metric-type>]/[<bucket>]/[<key>]?[versionId=<version-id>]`

Where:

- `<extension-type>` currently supports only `crr` for replication metrics
- `<location-name>` represents any current destination replication locations you
   have defined. To display metrics for all locations, use `all`
- `<metric-type>` is an optional field. If you specify a metric type, Backbeat
   returns the specified metric. If you omit it, Backbeat returns all available
   metrics for the given extension and location.
- `<bucket>` is an optional field. It carries the name of the bucket in which
   the object is expected to exist.
- `<key>` is an optional field. When getting CRR metrics for a particular object,
   it contains the object's key.
- `<version-id>` is an optional field. When getting CRR metrics for a particular
   object, it contains the object's version ID.

### `/_/backbeat/api/metrics/crr/<location-name>`

This route gathers all the metrics below, returning one JSON object for the
specified extension type and location name.

### `/_/backbeat/api/metrics/crr/<location-name>/pending`

This route returns the pending operations in number of objects and number
of total bytes for the specified extension type and location. This metric does
not expire.

**Example Output**:

```
"pending":{
    "description":"Number of pending replication operations (count) and bytes
    (size)",
    "results":{
        "count":"1",
        "size":"1024"
    }
}
```

### `/_/backbeat/api/metrics/crr/<location-name>/backlog`

This route returns the replication backlog in number of objects and number of
total bytes for the specified extension type and location name. Replication
backlog represents the objects that have been queued for replication to another
location, but for which the replication task is not complete. If replication
for an object fails, failed object metrics are considered backlog.

**Example Output**:

```
"backlog":{
    "description":"Number of incomplete replication operations (count) and
    number of incomplete bytes transferred (size)",
    "results":{
        "count":"1",
        "size":"1024"
    }
}
```

### `/_/backbeat/api/metrics/crr/<location-name>/completions`

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
        "count":"1",
        "size":"1024"
    }
}
```

### `/_/backbeat/api/metrics/crr/<location-name>/failures`

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
        "count":"1",
        "size":"1024"
    }
}
```

### `/_/backbeat/api/metrics/crr/<location-name>/throughput`

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

### `/_/backbeat/api/metrics/crr/<site-name>/progress/<bucket>/<key>?versionId=<version-id>`

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

### `/_/backbeat/api/metrics/crr/<site-name>/throughput/<bucket>/<key>?versionId=<version-id>`

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

For basic metrics, eight data points are collected:

- number of operations (ops)
- number of pending operations (opspending)
- number of completed operations (opsdone)
- number of failed operations (opsfail)
- number of bytes (bytes)
- number of pending bytes (bytespending)
- number of completed bytes (bytesdone)
- number of failed bytes (bytesfail)

To collect metrics, a separate Kafka Producer and Consumer pair
(`MetricsProducer` and `MetricsConsumer`) using their own Kafka topic
(default to "backbeat-metrics") produce their own Kafka entries.

When a new CRR entry is sent to Kafka, a Kafka entry to the metrics topic
is produced, indicating to increase `ops` and `bytes`. On consumption of this
metrics entry, Redis keys are generated with the following schema:

Site-level pending CRR metrics Redis key:
`<site-name>:<default-metrics-key>:<opspending-or-bytespending>`

Site-level CRR metrics Redis key:
`<site-name>:<default-metrics-key>:<ops-or-bytes>:<normalized-timestamp>`

Object-level CRR metrics Redis key:
`<site-name>:<bucket-name>:<key-name>:<version-id>:<default-metrics-key>:<ops-or-bytes>:<normalized-timestamp>`

A normalized timestamp determines the time interval on which to set the data.
The default metrics key ends with the type of data point it represents.

When the CRR entry is consumed from Kafka, processed, and the metadata for
replication status updated to a completed state (i.e. COMPLETED, FAILED),
a Kafka entry is sent to the metrics topic indicating to increase `opsdone` and
`bytesdone` if replication was successful or `opsfail` and `bytesfail` if
replication was unsuccessful. Again, on consumption of this metrics entry,
Redis keys are generated for their respective data points.

It is important to note that a `MetricsProducer` is initialized and producing
to the metrics topic both when the CRR topic `BackbeatProducer` produces and
sends a Kafka entry, and when the CRR topic `BackbeatConsumer` consumes and
processes its Kafka entries. The `MetricsConsumer` processes these Kafka metrics
entries and produces to Redis.

A single-location CRR entry produces up to eight keys in total. The data points
stored in Redis are saved in intervals (default of 5 minutes) and are available
up to an expiry time (default of 24 hours), with the exception of pending keys
which are not bound to an interval and do not expire.

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
