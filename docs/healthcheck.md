# Zenko Backbeat Healthcheck

Zenko Backbeat exposes a healthcheck route that returns a response with an HTTP
code.

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
|          |   components: the api server, Kafka, Zookeeper, or one of the    |
|          |   Producers for a topic                                          |
+----------+------------------------------------------------------------------+
```

## Routes

### `/_/backbeat/api/healthcheck`

Basic healthchecks return details on the health of Kafka and its topics.

If the response is not an HTTP error, it is structured as an object with two
main keys: *topics* and *internalConnections*.

The `topics` key returns details on the Kafka CRR topic only. The `name` field
returns the Kafka topic name, and the `partitions` field returns details of each
partition for the CRR topic. The `id` is the partition id, the `leader` is the
current node responsible for all reads and writes for the given partition, the
`replicas` array is the list of nodes that replicate the log for the given
partition, and the `isrs` array is the list of in-sync replicas.

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

The `internalConnections` key returns general details on the health of the
system as a whole. `isrHealth` checks if the minimum in-sync replicas for every
partition is met. The `zookeeper` field checks if ZooKeeper is running
properly. The `kafkaProducer` field checks the health of all Kafka Producers
for every topic.

The `zookeeper` details field provides a status code and status name provided
directly from the [node-zookeeper](https://github.com/alexguan/node-zookeeper-client#state)),
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

**Example Output**:
(**Note:** The following example is redacted for brevity.)

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
