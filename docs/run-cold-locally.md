<!-- markdownlint-disable MD013 -->

# S3 Lifecycle transition from Ring S3C to AWS S3 location

## Kafka and Zookeeper

### Install kafka and Zookeeper

### Start kafka and zookeeper servers

```sh
mkdir ./kafka && \
cd ./kafka && \
curl https://archive.apache.org/dist/kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz | tar xvz && \
sed 's/zookeeper.connect=.*/zookeeper.connect=localhost:2181\/backbeat/' \
kafka_2.11-0.11.0.0/config/server.properties > \
kafka_2.11-0.11.0.0/config/server.properties.backbeat
zookeeper-server-start kafka_2.11-0.11.0.0/config/zookeeper.properties
kafka-server-start kafka_2.11-0.11.0.0/config/server.properties.backbeat
```

## Mongo DB: deploy replica set

CloudServer uses Mongo DB as a metadata backend.
Vault is also using Mongo DB as a backend.

From: https://docs.mongodb.com/manual/tutorial/deploy-replica-set/

Create the necessary data directories for each member by issuing a
command similar to the following:

```sh
mkdir -p /tmp/mongodb/rs0-0 /tmp/mongodb/rs0-1 /tmp/mongodb/rs0-2
```

Start your mongod instances in their own shell windows by issuing the following commands:

```sh
mongod --replSet rs0 --port 27018 --bind_ip localhost --dbpath /tmp/mongodb/rs0-0 --oplogSize 128
mongod --replSet rs0 --port 27019 --bind_ip localhost --dbpath /tmp/mongodb/rs0-1 --oplogSize 128
mongod --replSet rs0 --port 27020 --bind_ip localhost --dbpath /tmp/mongodb/rs0-2 --oplogSize 128
```

Connect to one of your mongod:

```sh
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

```sh
rm -rf /tmp/mongodb/rs0-0/*  /tmp/mongodb/rs0-1/* /tmp/mongodb/rs0-2/*
```

## CloudServer

Add the following item to the `replicationEndpoints` section in the `config.json`.
It is the location name we will transition our data to.

```
{
    "site": "location-dmf-v1",
    "type": "dmf"
}
```

Update `locationConfig.json` with the following locations:

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
    "location-dmf-v1": {
        "type": "dmf",
        "objectId": "location-dmf-v1",
        "legacyAwsBehavior": false,
        "isCold": true,
        "details": {
            "endpoint": "ws://localhost:5001/session",
		    "username": "user1",
		    "password": "pass1",
		    "repoId": [
                "233aead6-1d7b-4647-a7cf-0d3280b5d1d7",
                "81e78de8-df11-4acd-8ad1-577ff05a68db"
            ],
		    "nsId": "65f9fd61-42fe-4a68-9ac0-6ba25311cc85"
        }
    }
}
```

```
REMOTE_MANAGEMENT_DISABLE=true S3VAULT=multiple S3DATA=multiple yarn start_mongo
```

## Vault2

sh
```
git clone https://github.com/scality/vault2
```

Add to `./config.json`

When we create an account, it seeds the defined role into the account.
The defined trust and permission policies will be attached to the role.

The lifecycle service user that we will create next will assume the role and
gain permission into the account.
The `permissionPolicy.Action` is defining what API operation is the assumer allowed to perform.
The `permissionPolicy.Resource` is defining on what bucket/prefix the action can be performed.

The `permissionPolicy.Action` should be much more granular.
We keep it wild for testing purposes.

```json
"accountSeeds": [
    {
        "role": {
            "roleName": "scality-role1",
            "trustPolicy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": { "AWS": "arn:aws:iam::000000000000:user/lifecycle" },
                        "Action": "sts:AssumeRole",
                        "Condition": {}
                    }
                ]
            }
        },
        "permissionPolicy": {
            "policyName": "scality-policy1",
            "policyDocument": {
                "Version": "2012-10-17",
                "Statement": [{
                    "Sid": "FullAccess",
                    "Effect": "Allow",
                    "Action": ["s3:*"],
                    "Resource": ["*"]
                }]
            }
        }
    }
]
```

```sh
VAULT_DB_BACKEND="MONGODB" yarn dev
```

## VaultClient

### Create a customer account

```sh
bin/vaultclient create-account --name account1 --email customer@account --port 8600
```

```sh
bin/vaultclient generate-account-access-key --name account1 --port 8600
```

```sh
aws configure --profile account1
```

### Create the management account

This management account will hold all the services accounts.
Those services user will assume the customer accounts's role to gain permission access.

```sh
bin/vaultclient create-account --name management --email dev@null --port 8600 --accountid 000000000000
```

```sh
bin/vaultclient generate-account-access-key --name management --port 8600
```

```sh
aws configure --profile management
```

### Create the lifecycle service user

NOTE: We could also use `bin/ensureServiceUser` to create the service user.

When creating the service user, we can make sure that the lifecycle service user's arn matches the
`accountSeeds.role.trustPolicy.Statement.Principal.AWS`
(i.e. `arn:aws:iam::000000000000:user/lifecycle`) defined in the `config.json` in the Vault section.
Indeed, the lifecycle service user is the only user trusted to assume the customer account's role.

```sh
aws iam create-user --user-name lifecycle --endpoint-url http://127.0.0.1:8600 --profile management
```

```sh
aws iam create-access-key --user-name lifecycle --endpoint-url http://127.0.0.1:8600 --profile management
```

```sh
aws configure --profile lifecycle
```

### Allow the lifecycle service user to assume roles

Create `assume.json` policy:

```json
{
   "Version": "2012-10-17",
   "Statement": {
     "Effect": "Allow",
     "Action": "sts:AssumeRole",
     "Resource": "*"
   }
 }
```

```sh
aws iam create-policy --policy-name assume --policy-document file://assume.json --endpoint-url http://127.0.0.1:8600 --profile management
```

```sh
aws iam attach-user-policy --policy-arn arn:aws:iam::000000000000:policy/assume --user-name lifecycle --endpoint-url http://127.0.0.1:8600 --profile management
```

### Create a lifecycle transition bucket

```sh
aws s3api create-bucket --bucket local-bucket --create-bucket-configuration LocationConstraint=us-east-1 --endpoint-url http://127.0.0.1:8000 --profile account1
```

Create `transition.json`

```json
{
    "Rules": [
        {
            "Status": "Enabled",
            "Prefix": "",
            "Transitions": [{
                "Days": 1, 
       		    "StorageClass": "location-dmf-v1"
            }],
            "ID": "456"
        }
    ]
}
```

```sh
aws s3api put-bucket-lifecycle-configuration --endpoint-url http://127.0.0.1:8000 --profile account1 --bucket local-bucket --lifecycle-configuration file://transition.json
```

## Backbeat

### Add a lifecycle conductor service user to retrieve the buckets’ account id

This user will assume its local role with the conductor policy.

```sh
AWS_PROFILE=management bin/ensureServiceUser apply backbeat-lifecycle-conductor -p extensions/lifecycle/conductor/policy.json --iam-endpoint http://127.0.0.1:8600 --sts-endpoint http://127.0.0.1:8800
```

```sh
aws configure --profile lifecycle-conductor
```

### Update `conf/config.json`

`extensions.lifecycle.auth`

```json
"auth": {
    "type": "assumeRole",
    "roleName": "scality-internal/scality-role1",
    "sts": {
        "host": "127.0.0.1",
        "port": 8800,
        "accessKey": "<lifecycle profile access key>",
        "secretKey": "<lifecycle profile secret key>"
    },
    "vault": {
        "host": "127.0.0.1",
        "port": 8600
    }
},
```

`extension.lifecycle.conductor.auth`

```json
"auth": {
    "type": "assumeRole",
    "roleName": "scality-internal/backbeat-lifecycle-conductor",
    "sts": {
        "host": "127.0.0.1",
        "port": 8800,
        "accessKey": "<lifecycle conductor access key>",
        "secretKey": "<lifecycle conductor secret key>"
    },
    "vault": {
        "host": "127.0.0.1",
        "port": 8600
    }
},
```

Update the `extension.lifecycle.conductor.cronRule` to run conductor's job every 10 seconds.

```json
"cronRule": "*/10 * * * * *",
```

Add the following to the `extension.lifecycle.conductor`

```json
"bucketSource": "mongodb",
"mongodb": {
    "replicaSetHosts":
        "localhost:27017,localhost:27018,localhost:27019",
    "writeConcern": "majority",
    "replicaSet": "rs0",
    "readPreference": "primary",
    "database": "metadata"
},
"vaultAdmin": {
    "host": "127.0.0.1",
    "port": 8500
},
```

<!-- Add the following to `extensions.replication.destination.bootstrapList`
It will be used by the queue_processor to check the site.

```json
{ "site": "aws-location", "type": "aws_s3" }
``` -->

Update `extensions.replication.source.auth`
TODO: We will soon support `"type": "assumeRole",`

```json
"auth": {
    "type": "account",
    "account": "account1",
    "vault": {
        "host": "127.0.0.1",
        "port": 8500,
        "adminPort": 8600
    }
}
```

Add coldStorageTopics inside the lifecycle extension `extensions.lifecycle`:

```json
"coldStorageTopics": ["cold-status-location-dmf-v1"],
```

Add the transitionProcessor inside the lifecycle extension `extensions.lifecycle`:

```json
"transitionProcessor": {
    "groupId": "backbeat-lifecycle-object-processor-group",
    "retry": {
        "maxRetries": 5,
        "timeoutS": 300,
        "backoff": {
            "min": 1000,
            "max": 300000,
            "jitter": 0.1,
            "factor": 1.5
        }
    },
    "concurrency": 10,
    "probeServer": {
        "bindAddress": "0.0.0.0",
        "port": 8556
    }
},
```

Update `extension.gc.auth`

```json
"auth": {
    "type": "assumeRole",
    "roleName": "scality-internal/scality-role1",
    "sts": {
        "host": "127.0.0.1",
        "port": 8800,
        "accessKey": "<lifecycle profile access key>",
        "secretKey": "<lifecycle profile secret key>"
    },
    "vault": {
        "host": "127.0.0.1",
        "port": 8600
    }
},
```

### Update conf/authdata.json with account1 informations and keys

TODO: When we support `"type": "assumeRole",` for replication, this step
will not be needed.

Note: To retreive account1 information, use vaulclient:
`bin/vaultclient get-account --account-name account1 --port 8600`

```json
{
    "accounts": [{
        "name": "account1",
        "arn": "arn:aws:iam::924286670056:/account1/",
        "canonicalID": "56a321265db286f6045e24cadae44a9f3874dd8deca559a16718f268031d8b18",
        "displayName": "account1",
        "keys": {
            "access": "MC50MQ6FNYPAVCM88PNW",
            "secret": "/lmgQTi5TqX1o2BMip42nReeecta15RvekzvBDlr"
        }
    }
    ]
}
```

### Run lifecycle conductor

```sh
yarn run lifecycle_conductor
```

### Run lifecycle bucket processor

```sh
EXPIRE_ONE_DAY_EARLIER=true TRANSITION_ONE_DAY_EARLIER=true REMOTE_MANAGEMENT_DISABLE=true yarn run lifecycle_bucket_processor
```

### Run lifecycle object transition processor

```sh
REMOTE_MANAGEMENT_DISABLE=true yarn run lifecycle_object_transition_processor
```

### Run garbage collector

```sh
REMOTE_MANAGEMENT_DISABLE=true yarn run garbage_collector
```

## Sorbet

### Run the DMF server (that will write to/read from tape) 

```sh
make
./bin/sorbetd --scheme=http --host=0.0.0.0 --port=5001
```

### Start sorbet

config.json should look like:

```json
{
    "command-timeout": "20s",
    "debug": false,
    "kafka-alter-topics": false,
    "kafka-archive-request-concurrency": 1,
    "kafka-archive-request-topic": "cold-archive-req-location-dmf-v1",
    "kafka-brokers": [
        "localhost:9092"
    ],
    "kafka-consumer-commit-batch-size": 100,
    "kafka-consumer-commit-interval": "3s",
    "kafka-consumer-group-id": "sorbet-forwarder-0001",
    "kafka-create-topics": true,
    "kafka-dead-letter-topic": "cold-dead-letter-location-dmf-v1",
    "kafka-gc-request-concurrency": 5,
    "kafka-gc-request-topic": "cold-gc-req-location-dmf-v1",
    "kafka-restore-request-concurrency": 5,
    "kafka-restore-request-topic": "cold-restore-req-location-dmf-v1",
    "kafka-status-response-topic": "cold-status-location-dmf-v1",
    "kafka-topic-partitions": 10,
    "kafka-topic-poll-interval": "1s",
    "kafka-topic-replication-factor": 1,
    "mongodb-collection": "__sorbetpendingjobs",
    "mongodb-database": "metadata",
    "mongodb-hosts": [
        "localhost:27018",
        "localhost:27019",
        "localhost:27020"
    ],
    "mongodb-replicaset": "rs0",
    "mongodb-sharding-enable": false,
    "ns-id": "65f9fd61-42fe-4a68-9ac0-6ba25311cc85",
    "pending-job-poll-after-age": "2m",
    "pending-job-poll-check-interval": "1m",
    "pending-job-store": "mongodb",
    "repo-id": [
        "233aead6-1d7b-4647-a7cf-0d3280b5d1d7",
        "81e78de8-df11-4acd-8ad1-577ff05a68db"
    ],
    "s3-access-key": "15G8SVFGJ3RMH32KIKUN",
    "s3-endpoint": "http://localhost:8000",
    "s3-secret-key": "mWt9kI7mwKJwOfOBvJzcP=+f6661HvQjm/Xg7Cyg",
    "s3-sts-archive-role-name": "scality-internal/scality-role1",
    "s3-sts-endpoint": "http://localhost:8800",
    "s3-sts-restore-role-name": "scality-internal/scality-role1",
    "s3-sts-session-name-prefix": "cold-",
    "trace-messages": false,
    "wait": false,
    "ws-endpoint": "ws://localhost:5001/session",
    "ws-job-poll-interval": "15m",
    "ws-keepalive-interval": "1m",
    "ws-password": "pass1",
    "ws-reuse": true,
    "ws-username": "user1"
}
```

```sh
./bin/sorbetctl forward kafka --config config.json
```

### Archive non version object: key1

```sh
aws s3api put-object --endpoint-url http://127.0.0.1:8000 --profile account1 --bucket local-bucket --key key1 --body package.json
```

Check that object metadata "StorageClass" has changed to "location-dmf-v1"

```sh
aws s3api head-object --endpoint-url http://127.0.0.1:8000 --profile account1 --bucket local-bucket --key key1
```

### Run queue_populator for restoring objects

First, add queuePopulator.auth to `queuePopulator` to backbeat's config.json

```json
"auth": {
    "type": "assumeRole",
    "roleName": "scality-internal/backbeat-lifecycle-conductor",
    "sts": {
        "host": "127.0.0.1",
        "port": 8800,
        "accessKey": "<lifecycle conductor access key>",
        "secretKey": "<lifecycle conductor secret key>"
    },
    "vault": {
        "host": "127.0.0.1",
        "port": 8600
    }
}
```

Then, create log offset Zookeeper node

```
zkCli -server localhost:2181/backbeat

create /queue-populator
```

Finally, for restoring objects, you will need to run the queue populator:

```sh
yarn run queue_populator
```

Let's restore object key1

```sh
aws s3api restore-object --endpoint-url http://127.0.0.1:8000 --profile account1 --restore-request Days=10 --bucket local-bucket --key key1
```
