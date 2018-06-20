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
`/_/metrics/<extension-type>/<site-name>/<metric-type>/<bucket>/<key>`

Where:

- `<extension-type>` can currently only support `crr` for replication metrics
- `<site-name>` represents any current sites you have defined. If you would like
  metrics displayed for all sites, use `all`
- `<metric-type>` is an optional field. Leaving this out will get all metrics
  available for given extension and site. If you specify a metric type, you will
  get the metric specified.
- `<bucket>` is an optional field, but is required if also specifying a key. If
  provided, it is the name of the bucket in which the object exists.
- `<key>` is an optional field. If provided, it is the object's key when getting
  CRR metrics for a particular object.

### `/_/metrics/crr/<site-name>`

This route gathers all the metrics below and returns as one JSON object for the
specified extension type and site name.

### `/_/metrics/crr/<site-name>/backlog`

This route returns the replication backlog in number of objects and number of
total MB for the specified extension type and site name. Replication backlog
represents the objects that have been queued up to be replicated to another
site, but the replication task has not been completed yet for that object.

**Example Output**:

```
"backlog":{
    "description":"Number of incomplete replication operations (count) and
    number of incomplete MB transferred (size)",
    "results":{
        "count":4,
        "size":"6.12"
    }
}
```

### `/_/metrics/crr/<site-name>/completions`

This route returns the replication completions in number of objects and number
of total MB transferred for the specified extension type and site name.
Completions are only collected up to an `EXPIRY` time, which is currently set
to **15 minutes**.

**Example Output**:

```
"completions":{
    "description":"Number of completed replication operations (count) and number
    of MB transferred (size) in the last 900 seconds",
    "results":{
        "count":31,
        "size":"47.04"
    }
}
```

### `/_/metrics/crr/<site-name>/throughput`

This route returns the current throughput in number of operations per second
(or number of objects replicating per second) and number of total MB completing
per second for the specified type and site name.

**Example Output**:

```
"throughput":{
    "description":"Current throughput for replication operations in ops/sec
    (count) and MB/sec (size)",
    "results":{
        "count":"0.00",
        "size":"0.00"
    }
}
```

### `/_/metrics/crr/<site-name>/progress/<bucket>/<key>`

This route returns replication progress in total MB transferred for the
specified object.

**Example Output**:

```
{
    "description": "Number of MB to be replicated (pending), number of MB
    transferred to the destination (completed), and percentage of the object
    that has completed replication (progress)",
    "pending": 13,
    "completed": 47,
    "progress": "78%"
}
```

### `/_/metrics/crr/<site-name>/throughput/<bucket>/<key>`

This route returns the throughput in number MB completing per second for the
specified object.

**Example Output**:

```
{
    "description": "Current throughput for object replication in MB/sec
    (throughput)",
    "throughput": 1
}
```

## Design

For basic metrics, only 4 data points are collected:

- number of operations (ops)
- number of completed operations (opsdone)
- number of bytes (bytes)
- number of completed bytes (bytesdone)

In order to collect metrics, a separate Kafka Producer and Consumer
(`MetricsProducer` and `MetricsConsumer`, respective) using its own Kafka topic
(default to "backbeat-metrics") will produce its own Kafka entries.

When a new CRR entry is sent to Kafka, a Kafka entry to the metrics topic will
be produced indicating to increase `ops` and `bytes`. On consumption of this
metrics entry, Redis keys will be generated following a format similar to:
`<site-name>:<bucket-name>:<key-name>:<default-metrics-key>:<ops-or-bytes>:<normalized-timestamp>`.
Normalized timestamp is used to determine in which time interval to set the data
on. The default metrics key will end with the type of data point it represents.

When the CRR entry is consumed from Kafka and processed, a Kafka entry is sent
to the metrics topic indicating to increase `opsdone` and `bytesdone`. Again,
on consumption of this metrics entry, Redis keys will be generated for their
respective data points.

It is important to note that a `MetricsProducer` is initialized and producing
to the metrics topic both when the CRR topic `BackbeatProducer` produces and
sends a Kafka entry, and when the CRR topic `BackbeatConsumer` consumes and
processes its respective Kafka entries. The `MetricsConsumer` will process these
Kafka metrics entries and produce to Redis.

A single site CRR entry should produce 4 keys in total. The data points stored
in Redis are saved in intervals (default of 5 minutes) and are available up to
an expiry time (default of 15 minutes).

An object CRR entry produces a single key. An initial key is set when the CRR
operation begins that stores the total size of the object to be replicated.
Then, for each part of the object that completes replication, another key is set
to reflect the total size of parts that have completed replication. The data
points stored in Redis are saved in intervals (default of 5 minutes) and are
available up to an expiry time (default of 24 hours).

A `BackbeatServer` (default port 8900) and `BackbeatAPI` expose these metrics
stored in Redis by querying based on the prepended Redis keys. Using these data
points, we can calculate simple metrics like backlog, number of completions,
progress, throughput, etc.
