# Zenko Backbeat Metrics

Zenko Backbeat exposes various metric routes that return a response with an
HTTP code.

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

`/_/metrics/<extension-type>/<site-name>/<metric-type>`

Where:

- `<extension-type>` can currently only support `crr` for replication metrics.
- `<site-name>` represents any current sites you have defined. To display
  metrics for all sites, use `all`.
- `<metric-type>` is an optional field. Leaving this out retrieves all available
  metrics for a given extension and site. Specifying a metric type returns the
  metric specified.

### Site Name

`/_/metrics/crr/<site-name>`

This route returns all metrics below, formatted as one JSON object for the
specified extension type and site name.

### Backlog

`/_/metrics/crr/<site-name>/backlog`

This route returns the replication backlog as a number of objects and a number
of total MB for the specified extension type and site name. The replication
backlog is the set of objects queued for replication to another site, but for
which replication is not yet complete.

#### Example Output

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

### Completions

 `/_/metrics/crr/<site-name>/completions`

This route returns replication completions as a number of objects and a number
of total bytes (in MB) transferred for the specified extension type and site
name. Completions are only collected up to an `EXPIRY` time, currently set to
**15 minutes**.

#### Example Output

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

### Throughput

`/_/metrics/crr/<site-name>/throughput`

This route returns the current throughput in number of operations per second
(or number of objects replicating per second) and number of total bytes (in MB)
completing per second for the specified type and site name.

#### Example Output

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

## Design

For basic metrics, only four data points are collected:

- Number of operations (ops)
- Number of completed operations (opsdone)
- Number of bytes (bytes)
- Number of completed bytes (bytesdone)

To collect metrics, a separate Kafka Producer and Consumer (`MetricsProducer`
and `MetricsConsumer`, respectively)—each using its own Kafka topic
(default to "backbeat-metrics")—produce their own Kafka entries.

When a new CRR entry is sent to Kafka, the queue populator sends a Kafka
entry to the metrics topic, signifying an increase in `ops` and `bytes`. On
consuming this metrics entry, the MetricsConsumer generates Redis keys in
the following format:
`<site-name>:<default-metrics-key>:<ops-or-bytes>:<normalized-timestamp>`
where "normalized-timestamp" determines the interval to set the data on.
The default metrics key ends with the type of data point it represents.

When the queue processor's internal MetricsProducer consumes and processes
the CRR entry from Kafka, it sends a Kafka entry to the metrics topic,
incrementing `opsdone` and `bytesdone`. When the MetricsConsumer consumes
this metrics entry, it generates Redis keys for the new data points.

It is important to note that a `MetricsProducer` is initialized and producing
to the metrics topic both when the `BackbeatProducer` CRR topic produces and
sends a Kafka entry *and* when the `BackbeatConsumer` CRR topic consumes and
processes its Kafka entries. The `MetricsConsumer` processes these
Kafka metrics and produces them to Redis.

A single-site CRR entry produces four keys. The data points stored
in Redis are saved in intervals (default: 5 minutes) and are available up to
an expiry time (default: 15 minutes).

A `BackbeatServer` (default port 8900) and `BackbeatAPI` expose these metrics
stored in Redis by querying based on the prepended Redis keys. This data
enables calculation of simple metrics like backlog, completion count,
and throughput.

