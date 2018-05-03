# Zenko Backbeat Metrics

Zenko Backbeat exposes various metric routes that return a response with an
HTTP code.

Metrics are currently available for the following service extensions:

- Cross-Region Replication (CRR)
- Metadata Ingestion

## Response Codes

```
+----------+------------------------------------------------------------------+
| Response | Details                                                          |
+==========|==================================================================+
| 200      | OK: Success                                                      |
+----------+------------------------------------------------------------------+
| 403      | AccessDenied: Requested IP address must be defined in            |
|          | 'conf/config.json' in field 'server.healthChecks.allowFrom'      |
+----------|------------------------------------------------------------------+
| 404      | RouteNotFound: Route must be valid.                              |
+----------|------------------------------------------------------------------+
| 405      | MethodNotAllowed: The HTTP verb must be GET.                     |
+----------|------------------------------------------------------------------|
| 500      | InternalError: This could be caused by one of several            |
|          | components: the API server, Kafka, Zookeeper, Redis, or one of   |
|          | the Producers for a topic.                                       |
+----------+------------------------------------------------------------------+
```

## Routes

Routes are organized as:

`/_/backbeat/api/metrics/<extension-type>/<location-name>/<metric-type>/[<bucket>]/[<key>]?[versionId=<version-id>]`

Where:

- `<extension-type>` can currently only support `crr` for replication
  metrics and `ingestion` for ingestion metrics
- `<location-name>` represents any current destination replication locations you
   have defined. To display metrics for all locations, use `all`.
- `<metric-type>` is an optional field **only for replication metrics**. If you
   specify a metric type, Backbeat returns the specified metric. If you omit it,
   Backbeat returns all available metrics for the given extension and location.
- `<bucket>` is an optional field. It carries the name of the bucket in which
   the object is expected to exist.
- `<key>` is an optional field. When getting CRR metrics for a particular object,
   it contains the object's key.
- `<version-id>` is an optional field. When getting CRR metrics for a particular
   object, it contains the object's version ID.

### Location Name

`/_/backbeat/api/metrics/crr/<location-name>`

This route gathers all the metrics below, returning one JSON object for the
specified extension type and location name.

### Pending

`/_/backbeat/api/metrics/<extension-type>/<location-name>/pending`

Where `<extension-type>` can be one of the following extensions:

- Cross-Region Replication: `crr`
- Metadata Ingestion: `ingestion`

This route returns the pending operations in number of objects for the specified
extension type and location. In addition, for replication only, number of total
bytes for the specified location is also provided.

This metric does not expire.

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

### Backlog

`/_/backbeat/api/metrics/crr/<location-name>/backlog`

This route returns the replication backlog as a number of objects and
number of total bytes for the specified extension type and location
name. The replication backlog is the set of objects queued for
replication to another location, but for which replication is not yet
complete. If replication for an object fails, failed object metrics
are considered as backlog.

#### Example Output

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

### Completions

`/_/backbeat/api/metrics/<extension-type>/<location-name>/completions`

This route returns the completions in number of objects for the specified
extension type and location. In addition, for replication only, number of total
bytes transferred for the specified location is also provided.
Completions are only collected up to an `EXPIRY` time, which is currently set
to **24 hours**.

#### Example Output

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

### CRR Failures

`/_/backbeat/api/metrics/crr/<location-name>/failures`

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

### Throughput

`/_/backbeat/api/metrics/<extension-type>/<location-name>/throughput`

Where `<extension-type>` can be one of the following extensions:

- Cross-Region Replication: `crr`
- Metadata Ingestion: `ingestion`

This route returns the current throughput in number of completed operations per
second (or number of objects replicating per second) for the specified extension
and location name. In addition, for replication only, number of total bytes
completing per second for the specified location is also provided.

Note throughput is averaged over the past 15 minutes of data collected so this
metric is really an average throughput.

#### Example Output

```
"throughput":{
    "description":"Current throughput for replication operations in ops/sec
    (count) and bytes/sec (size) in the last 86400 seconds",
    "results":{
        "count":"0.00",
        "size":"0.00"
    }
}
```

### Progress

`/_/backbeat/api/metrics/crr/<location-name>/progress/<bucket>/<key>?versionId=<version-id>`

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

## Design

### Cross-Region Replication (CRR)

For basic metrics, eight data points are collected:

- Number of operations (ops)
- Number of pending operations (opspending)
- Number of completed operations (opsdone)
- Number of failed operations (opsfail)
- Number of bytes (bytes)
- Number of pending bytes (bytespending)
- Number of completed bytes (bytesdone)
- Number of failed bytes (bytesfail)

To collect metrics, a separate Kafka Producer and Consumer pair
(`MetricsProducer` and `MetricsConsumer`) using their own Kafka topic
(default to "backbeat-metrics") produce their own Kafka entries.

When a new CRR entry is sent to Kafka, a Kafka entry to the metrics topic
is produced, indicating to increase `ops` and `bytes`. On consumption of this
metrics entry, Redis keys are generated with the following schema:

Location-level pending CRR metrics Redis key:
`<location-name>:<default-metrics-key>:<opspending-or-bytespending>`

Location-level CRR metrics Redis key:
`<location-name>:<default-metrics-key>:<ops-or-bytes>:<normalized-timestamp>`

Object-level CRR metrics Redis key:
`<location-name>:<bucket-name>:<key-name>:<version-id>:<default-metrics-key>:<ops-or-bytes>:<normalized-timestamp>`

A normalized timestamp determines the time interval on which to set the data.
The default metrics key ends with the type of data point it represents.

When the CRR entry is consumed from Kafka, processed, and the metadata for
replication status updated to a completed state (i.e. COMPLETED, FAILED),
a Kafka entry is sent to the metrics topic indicating to increase `opsdone` and
`bytesdone` if replication was successful or `opsfail` and `bytesfail` if
replication was unsuccessful. Again, on consumption of this metrics entry,
Redis keys are generated for their respective data points.

It is important to note that a `MetricsProducer` is initialized and producing
to the metrics topic both when the `BackbeatProducer` CRR topic produces and
sends a Kafka entry, and when the `BackbeatConsumer` CRR topic consumes and
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

### Metadata Ingestion

Only the number of operations pending and operations completed are collected
for ingestion.

To collect metrics, a separate Kafka Producer and Consumer pair
(`MetricsProducer` and `MetricsConsumer`) using their own Kafka topic
(default to "backbeat-metrics") produce their own Kafka entries.

When a new ingestion entry is sent to Kafka, a Kafka entry to the metrics topic
is produced, indicating to increase `opsPending`. On consumption and completion
of processing this ingestion entry, another Kafka entry is sent to the metrics
topic, indicated to decrease `opsPending` and increase `opsDone`.

On consumption of metrics entries, Redis keys are generated with the following
schema:

Location-level Ingestion metrics Redis key for completions:
`<location-name>:<default-metrics-key>:opsDone:<normalized-timestamp>`

Location-level Ingestion metrics Redis key for pending:
`<location-name>:<default-metrics-key>:pending`

Pending keys do not expire and do not have a timestamp.

A normalized timestamp determines the time interval on which to set the data.
The default metrics key ends with the type of data point it represents.

Using the number of operations completed, we can determine ingested object
completions and determine an average (default over 15 minutes) throughput of
operations per second.

A `BackbeatServer` (default port 8900) and `BackbeatAPI` expose these metrics
stored in Redis by querying based on the prepended Redis keys. This data
enables calculation of simple metrics like backlog, completion count,
and throughput.
