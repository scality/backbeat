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

Please refer to the ****[Design document](/DESIGN.md)****

- [CRR from CloudServer to AWS S3 workflow](/docs/crr-to-aws-s3.md)

## QUICKSTART

This guide assumes the following:

* `node` is installed (check package.json for supported versions)
* `yarn` is installed (anything recent)
* `aws cli` is installed (latest)

### Run Kafka and Zookeeper

#### Install Kafka and Zookeeper

Follow the [quick start guide](https://kafka.apache.org/quickstart) to get up and running.
Be sure to create a Kafka topic, the guide here uses the name of `backbeat-replication`.

#### Create a zookeeper node and kafka topic

Use the kafka provided zookeeper shell located in the bin directory:

```
$ bin/zookeeper-shell.sh localhost:2181
```

Create the `replication-populator` node:

```
create /replication-populator my_data
```

We may leave the zookeeper server now:

```
quit
```

### Run Scality Components

#### Start Vault and Scality Cloudserver

1. Run an instance of [Vault](https://github.com/scality/vault)
    * Vault is a private repository so you will need access
1. Run an instance of [Cloudserver](https://github.com/scality/cloudserver)
    * Be sure to configure cloudserver to use Vault
1. Create a vault account and credentials using the [Vault Client](https://github.com/scality/vaultclient)
    * Note: Vault client is included as a dependency and can be ran from
    `node_modules/vaultclient/bin/vaultclient`
    * The examples below use the username of `backbeatuser`
    * Be sure to include this new user in your aws credentials

#### Setup replication with backbeat

Set up replication on your buckets:

```
node bin/replication.js setup \
  --source-bucket source-bucket \
  --source-profile backbeatuser \
  --target-bucket target-bucket \
  --target-profile backbeatuser
```

Run the backbeat queue populator:

```
yarn queue_populator
```

In a new shell, run the backbeat queue processor:

```
yarn queue_processor SITE
```

You are now ready to put data on `source-bucket` and watch it replicate to `target-bucket`!

Put an object on the `source-bucket`:

```
echo 'content to be replicated' > replication_contents && \
aws s3api put-object \
--bucket source-bucket \
--key object-to-replicate \
--body replication_contents \
--endpoint http://localhost:8000 \
--profile backbeatuser
```

Check the replication status of the object we have just put:

```
aws s3api head-object \
--bucket source-bucket \
--key object-to-replicate \
--endpoint http://localhost:8000 \
--profile backbeatuser
```

The object's "ReplicationStatus" should either be "PENDING", or if some time has
passed, then it should be "COMPLETED".

Check if the object has been replicated to the target bucket:

```
aws s3api head-object \
--bucket target-bucket \
--key object-to-replicate \
--endpoint http://localhost:8000 \
--profile backbeatuser
```

After some time, the object's "ReplicationStatus" should be "REPLICA".
