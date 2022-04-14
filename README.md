# Zenko Backbeat

![backbeat logo](res/backbeat-logo.png)

## OVERVIEW

Backbeat is an engine with a messaging system at its heart.
It's part of Zenko, [Scality](http://www.scality.com/)’s
Open Source Multi-Cloud Data Controller.
Learn more about Zenko at [Zenko.io](http://www.zenko.io/)

Backbeat is optimized for queuing metadata updates and dispatching work
to long-running tasks in the background.
The core engine can be extended for many use cases,
which are called extensions, as listed below.

## EXTENSIONS

### Asynchronous Replication

This feature replicates objects from one S3 bucket to another S3
bucket in a different geographical region. The extension uses the
local Metadata journal as the source of truth and replicates object
updates in a FIFO order.

## DESIGN

- [Backbeat core design](/DESIGN.md)
- [CRR to AWS S3 workflow](/docs/crr-to-aws-s3.md)
- [Object Lifecycle management](/docs/lifecycle.md)
- [Metrics](/docs/metrics.md)
- [CRR Pause and Resume](/docs/pause-resume.md)
- [Healthcheck](/docs/healthcheck.md)
- [Site Level CRR](/docs/site-level-crr.md)
- [Transient Source](/docs/transient-crr-source.md)
- [Out of Band updates from RING](/docs/oob-s3-ring.md)

## QUICKSTART

This guide assumes the following:

* Using MacOS
* `brew` is installed (get it [here](https://brew.sh/))
* `node` is installed (version 6.9.5)
* `yarn` is installed (version 3.10.10)
* `aws` is installed (version 1.11.1)

### Kafka and Zookeeper

#### Install and start kafka and zookeeper servers

```
mkdir ~/kafka && \
cd ~/kafka && \
curl https://archive.apache.org/dist/kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz | \
tar xvz && \
sed 's/zookeeper.connect=.*/zookeeper.connect=localhost:2181\/backbeat/' \
kafka_2.11-0.11.0.0/config/server.properties > \
kafka_2.11-0.11.0.0/config/server.properties.backbeat
```

Start the zookeeper server:

```
zookeeper-server-start kafka_2.11-0.11.0.0/config/zookeeper.properties
```

In a different Shell start the kafka server:

```
kafka-server-start kafka_2.11-0.11.0.0/config/server.properties.backbeat
```

#### Install and run Redis on port 6379 (default port)

The "Failed Entry Processor" (started by Queue Populator) stores
the "failed replication entries" to the Redis sorted set.

#### Create log offset Zookeeper node

```
zkCli -server localhost:2181/backbeat

create /queue-populator
```

#### Create the kafka topics

##### backbeat-replication

```
kafka-topics --create \
--zookeeper localhost:2181/backbeat \
--replication-factor 1 \
--partitions 1 \
--topic backbeat-replication
```

##### backbeat-replication-status

```
kafka-topics --create \
--zookeeper localhost:2181/backbeat \
--replication-factor 1 \
--partitions 1 \
--topic backbeat-replication-status
```

##### backbeat-replication-failed

```
kafka-topics --create \
--zookeeper localhost:2181/backbeat \
--replication-factor 1 \
--partitions 1 \
--topic backbeat-replication-failed
```

### Vault

```
git clone https://github.com/scality/Vault ~/replication/vault && \
cd ~/replication/vault && \
yarn i
```

Run vault with "in-memory backend"

```
chmod 400 ./tests/utils/keyfile

VAULT_DB_BACKEND="MEMORY" yarn start
```

> :warning: **with "in-memory backend"**, all data is lost after you stop the process.

or with a "mongodb backend"

```
chmod 400 ./tests/utils/keyfile

VAULT_DB_BACKEND="MONGODB" yarn start
```

> mongodb can be installed follwing these [steps](https://github.com/scality/backbeat/blob/development/8.3/docs/run-oob-locally.md#mongo-db-deploy-replica-set)

### CloudServer

```
git clone https://github.com/scality/cloudserver ~/replication/cloudserver && \
cd ~/replication/cloudserver && \
yarn i
```

Generate AWS access/secret key with full IAM and S3 access.

Add your keys `aws configure --profile aws-account`

Create AWS destination versioning-enabled bucket.

```sh
aws s3api create-bucket --bucket <DESTINATION_BUCKET_NAME> --profile aws-account

aws s3api put-bucket-versioning \
--bucket <DESTINATION_BUCKET_NAME> \
--versioning-configuration Status=Enabled \
--profile aws-account
```

Replace existing `./locationConfig.json` with:

```json
{
    "us-east-1": {
      "details": {
        "supportsVersioning": true
      },
      "isTransient": false,
      "legacyAwsBehavior": false,
      "objectId": "0b1d9226-a694-11eb-bc21-baec55d199cd",
      "type": "file"
    },
    "aws-location": {
        "type": "aws_s3",
        "legacyAwsBehavior": true,
        "details": {
            "awsEndpoint": "s3.amazonaws.com",
            "bucketName": "<DESTINATION_BUCKET_NAME>",
            "bucketMatch": false,
            "credentialsProfile": "aws-account",
            "serverSideEncryption": true
        }
    }
}
```

Update `./config.json` with

```json
"replicationEndpoints": [{ "site": "aws-location", "type": "aws_s3" }],
```

In `./config.json`, make sure `recordLog.enabled` is set to `true`

```
"recordLog": {
        "enabled": true,
        ...
}
```

Run Cloudserver

```
S3DATA=multiple S3METADATA=mongodb REMOTE_MANAGEMENT_DISABLE=true \
S3VAULT=scality yarn start
```

### Vaultclient

Create a "Zenko" account and generate keys

```
bin/vaultclient create-account --name bart --email dev@backbeat --port 8600
bin/vaultclient generate-account-access-key --name bart --port 8600
aws configure --profile bart
```

### Backbeat

```
git clone https://github.com/scality/backbeat ~/replication/backbeat && \
cd ~/replication/backbeat && \
yarn i
```

Update `conf/authdata.json` with bart informations and keys.

```json
{
    "accounts": [{
        "name": "bart",
        "arn": "aws:iam::331457510670:/bart",
        "canonicalID": "2083781e15384e30f48c651a948ec2dc1e1801c4af24c2750a166823e28ca570",
        "displayName": "bart",
        "keys": {
            "access": "20TNCD06HOCSLQSABFZP",
            "secret": "1P43SL0ekJjXnQvliV0KgMibZ=N2lKZO4dpnWzbF"
        }
    }
    ]
}
```

Update `conf/config.json` section `extensions.replication.source.auth`

```json
"auth": {
    "type": "account",
    "account": "bart",
    "vault": {
        "host": "127.0.0.1",
        "port": 8500,
        "adminPort": 8600
    }
}
```

Make sure `conf/config.json` section
`extensions.replication.destination.bootstrapList` includes:

```json
{ "site": "aws-location", "type": "aws_s3" }
```

#### Queue populator

```
S3_REPLICATION_METRICS_PROBE=true REMOTE_MANAGEMENT_DISABLE=true \
yarn run queue_populator
```

#### Queue processor

```
S3_REPLICATION_METRICS_PROBE=true REMOTE_MANAGEMENT_DISABLE=true \
yarn run queue_processor aws-location
```

#### Replication status processor

```
S3_REPLICATION_METRICS_PROBE=true REMOTE_MANAGEMENT_DISABLE=true \
yarn run replication_status_processor
```

### AWS S3 CLI

Create a source bucket with versioning enabled:

```
aws s3api create-bucket \
--bucket sourcebucket \
--endpoint-url http://127.0.0.1:8000 \
--profile bart
```

```
aws s3api put-bucket-versioning \
--bucket sourcebucket \
--versioning-configuration Status=Enabled \
--endpoint-url=http://127.0.0.1:8000 \
--profile bart
```

#### Set up replication

Create `replication.json`

```json
{
    "Role": "arn:aws:iam::root:role/s3-replication-role",
    "Rules": [
        {
            "Status": "Enabled",
            "Prefix": "",
            "Destination": {
                "Bucket": "arn:aws:s3:::sourcebucket",
                "StorageClass": "aws-location"
            }
        }
    ]
}
```

```
aws s3api put-bucket-replication \
--bucket sourcebucket \
--replication-configuration file://replication.json \
--endpoint-url=http://127.0.0.1:8000 \
--profile bart
```

#### Put object to be replicated

```
aws s3api put-object \
--key key0 \
--body file \
--bucket sourcebucket \
--endpoint-url=http://127.0.0.1:8000 \
--profile bart
```

#### Check that object has been replicated

```
aws s3api head-object \
--key bucketsource/key0 \
--bucket <DESTINATION_BUCKET_NAME> \
--profile aws-account
```

#### Structure

In our `$HOME` directory, we now have the following directories:

```
$HOME
├── kafka
│   └── kafka_2.11-0.11.0.0
├── replication
    ├── backbeat
    ├── cloudserver
    └── vault
```

[badgepriv]: http://ci.ironmann.io/gh/scality/backbeat.svg?style=svg&circle-token=32e5dfd968e673450c44f0a255d1a812bae9b00c
[badgepub]: https://circleci.com/gh/scality/backbeat.svg?style=svg
