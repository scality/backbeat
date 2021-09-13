<!-- markdownlint-disable MD013 -->

# OOB update locally on MacOS

## Kafka

### Install kafka

```
brew install kafka && brew install zookeeper
```

### Start kafka and zookeeper servers

```
mkdir ~/kafka && \
cd ~/kafka && \
curl https://archive.apache.org/dist/kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz | tar xvz && \
sed 's/zookeeper.connect=.*/zookeeper.connect=localhost:2181\/backbeat/' \
kafka_2.11-0.11.0.0/config/server.properties > \
kafka_2.11-0.11.0.0/config/server.properties.backbeat
zookeeper-server-start kafka_2.11-0.11.0.0/config/zookeeper.properties
kafka-server-start kafka_2.11-0.11.0.0/config/server.properties.backbeat
```

### Watch and clean ingestion state (Optional)

```
zkCli -server localhost:2181/backbeat
ls /backbeat/ingestion/state
delete /backbeat/ingestion/state/<location>
```

## Mongo DB: deploy replica set

CloudServer uses Mongo DB as a metadata backend.
Vault is also using Mongo DB as a backend.

From: https://docs.mongodb.com/manual/tutorial/deploy-replica-set/

Create the necessary data directories for each member by issuing a
command similar to the following:

```
mkdir -p /tmp/mongodb/rs0-0 /tmp/mongodb/rs0-1 /tmp/mongodb/rs0-2
```

Start your mongod instances in their own shell windows by issuing the following commands:

```
mongod --replSet rs0 --port 27018 --bind_ip localhost --dbpath /tmp/mongodb/rs0-0 --oplogSize 128
mongod --replSet rs0 --port 27019 --bind_ip localhost --dbpath /tmp/mongodb/rs0-1 --oplogSize 128
mongod --replSet rs0 --port 27020 --bind_ip localhost --dbpath /tmp/mongodb/rs0-2 --oplogSize 128
```

Connect to one of your mongod:

```
mongo --port 27018
```

initiate the replica set:
You can create a replica set configuration object in mongo environment,
as in the following example:

```
rsconf = { _id: "rs0", members: [ { _id: 0, host: "localhost:27018" }, { _id: 1, host: "localhost:27019" }, { _id: 2, host: "localhost:27020" } ] }
```

use `rs.initiate()` to initiate the replica set:

```
rs.initiate( rsconf )
```

(SKIP THIS STEP - Only use it to clean up your mongo) Clean up:

```
rm -rf /tmp/mongodb/rs0-0/*  /tmp/mongodb/rs0-1/* /tmp/mongodb/rs0-2/*
```

## CloudServer

/!\ IMPORTANT: locationConfig.json should be identical to the backbeat conf/locationConfig.json.
Add Ring access key and secret key in locationConfig.json:
https://github.com/scality/backbeat/blob/1657782020009d63be8f3df342917305dea7671b/conf/locationConfig.json

```
REMOTE_MANAGEMENT_DISABLE=true S3VAULT=multiple yarn start_mongo
```

## Vault

```
VAULT_DB_BACKEND="MONGODB" yarn start
```

## Redis

Note: Redis is used for its “publish/subscribe messages” feature.
Client sends an authenticated request (ex. resume ingestion for location1)
to CloudServer that gets proxied to Backbeat API server.
Backbeat API will publish messages to “backbeat-ingestion-location1”
channel with a “resume” state. Ingestion producer subscribes
to “backbeat-ingestion-*”, receives messages and updates application and zookeeper state.

```
redis-server /usr/local/etc/redis.conf
```

## Backbeat - Backbeat API

```
git checkout feature/ARTESCA-1849/poc
```

Add Ring access key and secret key in conf/locationConfig.json:

```
"accessKey": "TO_BE_ADDED",
"secretKey": "TO_BE_ADDED"
```

Start backbeat API:

```
MANAGEMENT_BACKEND=operator REMOTE_MANAGEMENT_DISABLE=true yarn start
```

## Backbeat - Ingestion producer

```
MANAGEMENT_BACKEND=operator yarn ingestion_populator
```

## Backbeat - Ingestion processor

```
MANAGEMENT_BACKEND=operator yarn mongo_queue_processor
```

## VaultClient

Create an IAM account (account1) and its keys:

```
bin/vaultclient create-account --name account1 --email dev@null1 --port 8600
bin/vaultclient generate-account-access-key --name account1 --port 8600
aws configure --profile account1
```

## AWS S3 CLI

Create a bucket into this account with ingestion enabled location:

```
aws s3api create-bucket \
    --bucket bucket1 \
    --create-bucket-configuration LocationConstraint=wontwork-location:ingest \
    --endpoint-url http://127.0.0.1:8000 \
    --profile account1
```

Check the source bucket exists and fetch its objects.

```
aws s3api list-objects --endpoint-url http://217.182.11.36 --profile s3consoleaccount --bucket ring-bucket-1
```

## Zenko-client

Resume ingestion for location “wontwork-location”
Note: by default locations ingestion state is set to paused.

```
git checkout feature/ARTESCA-1849/poc
node oob.js
```

## AWS S3 CLI

Check objects in bucket source have been ingested in bucket1:

```
aws s3api list-objects --bucket bucket1 --endpoint-url http://127.0.0.1:8000 --profile account1
```
