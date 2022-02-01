# Replication with CRR replay

## Kafka and Zookeeper

### Install and start kafka and zookeeper servers

```
mkdir ~/kafka && \
cd ~/kafka && \
curl https://archive.apache.org/dist/kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz | \
tar xvz && \
sed 's/zookeeper.connect=.*/zookeeper.connect=localhost:2181\/backbeat/' \
kafka_2.11-0.11.0.0/config/server.properties > \
kafka_2.11-0.11.0.0/config/server.properties.backbeat

zookeeper-server-start kafka_2.11-0.11.0.0/config/zookeeper.properties

kafka-server-start kafka_2.11-0.11.0.0/config/server.properties.backbeat
```

## CloudServer

```
git checkout development/7.4
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

Run Cloudserver

```
S3DATA=multiple S3METADATA=file REMOTE_MANAGEMENT_DISABLE=true \
S3VAULT=scality yarn start
```

## Vault

```
git checkout development/7.4
```

Run vault with "in-memory backend"

```
VAULT_DB_BACKEND="MEMORY" yarn start
```

Warning: with "in-memory backend", all data is lost after you stop the process.

## Backbeat

```
git checkout development/7.4
```

Create a "Zenko" account and generate keys

```
bin/vaultclient create-account --name bart --email dev@backbeat --port 8600
bin/vaultclient generate-account-access-key --name bart --port 8600
aws configure --profile bart
```

Update `conf/authdata.json` with bart informations and keys.

```json
{
    "accounts": [{
        "name": "bart",
        "arn": "arn:aws:iam::331457510670:/bart/",
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

```
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

### Queue populator

```
yarn run queue_populator
```

### Queue processor

```
yarn run queue_processor
```

### Queue processor

```
yarn run queue_processor
```

### Replication status processor

```
S3_REPLICATION_METRICS_PROBE=true yarn run replication_status_processor
```

### Replication replay processor

For location: `aws-location` and topic: `backbeat-replication-replay-0`

```
yarn run replay_processor aws-location
```

### Replication replay processor (topic: backbeat-replication-replay-1)

For location: `aws-location` and topic: `backbeat-replication-replay-1`

```
yarn run replay_processor aws-location backbeat-replication-replay-1
```

### Replication replay processor (topic: backbeat-replication-replay-2)

For location: `aws-location` and topic: `backbeat-replication-replay-2`

```
yarn run replay_processor aws-location backbeat-replication-replay-2
```

## AWS S3 CLI

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

### Set up replication

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

### Put object to be replicated

```
aws s3api put-object \
--key key0 \
--body file \
--bucket sourcebucket \
--endpoint-url=http://127.0.0.1:8000 \
--profile bart
```

### Check that object has been replicated

```
aws s3api head-object \
--key bucketsource/key0 \
--bucket <DESTINATION_BUCKET_NAME> \
--profile aws-account
```
