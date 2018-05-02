# Zenko Backbeat Healthcheck

Zenko Backbeat exposes a healthcheck endpoint that responds to queries with
HTTP code.

## Response Codes

| Response | Details   |
|:-----|:--------------  |
| 200 | OK: success    |
| 403 | AccessDenied: Requested IP address must be defined in 'conf/config.json' in field 'server.healthChecks.allowFrom'. |
| 404 | RouteNotFound: Route must be valid. |
| 405 | MethodNotAllowed: The HTTP verb must be a GET. |
| 500 | InternalError: This could be caused by one of several components: the API server, Kafka, Zookeeper, or one of the Producers for a topic.

## Routes

### `/_/healthcheck`

Basic healthchecks return details on the health of Kafka and its topics.

A successful response is structured as an object with two main keys:
topics and internalConnections.

* The `topics` key returns details on the Kafka CRR topic only.
  * The `name` field returns the Kafka topic name, and
  * The `partitions` field returns details of each partition for the CRR topic,
    where:
    * `id` is the partition id.
    * `leader` is the current node responsible for all reads and writes for
      the given partition.
    * `replicas` is an array containing a list of nodes that replicates the
      log for the given partition.
    * `isrs` is an array listing in-sync replicas.

```
topics: {
    <topicName>: {
        name: <string>,
        partitions: {
            id: <number>,
            leader: <number>,
            replicas: [<number>, ...],
            isrs: [<number>, ...],
        },
        ...
    },
}
```

* `internalConnections` returns general details on the whole system's health.
  * `isrHealth` indicates if the minimum in-sync replicas for every partition
    are met.
  * `zookeeper` indicates if ZooKeeper is running properly.
  * `kafkaProducer` reports the health of all Kafka Producers for
    every topic.
  * `zookeeper` contains a status code and status name provided directly from the
    [node-zookeeper](https://github.com/alexguan/node-zookeeper-client#state)),
    library.

```
internalConnections: {
    isrHealth: <ok || error>,
    zookeeper: {
        status: <ok || error>,
        details: {
            name: <value>,
            code: <value>
        }
    },
    kafkaProducer: {
        status: <ok || error>
    }
}
```

**Example Output**

(NOTE: This example is condensed for brevity.)

```
{
    "topics": {
        "backbeat-replication": {
            "name": "backbeat-replication",
            "partitions": [
                {
                    "id": 2,
                    "leader": 4,
                    "replicas": [2,3,4],
                    "isrs": [4,2,3]
                },
                ...
                {
                    "id": 0,
                    "leader": 2,
                    "replicas": [0,1,2],
                    "isrs": [2,0,1]
                }
            ]
        }
    },
    "internalConnections": {
        "isrHealth": "ok",
        "zookeeper": {
            "status": "ok",
            "details": {
                "name": "SYNC_CONNECTED",
                "code": 3
            }
        },
        "kafkaProducer": {
            "status": "ok"
        }
    }
}
```
