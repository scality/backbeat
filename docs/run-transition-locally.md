<!-- markdownlint-disable MD013 -->

# S3 Lifecycle transition from Ring S3C to AWS S3 location

## Kafka and Zookeeper

### Install kafka and Zookeeper

### Start kafka and zookeeper servers

```sh
mkdir ~/kafka && \
cd ~/kafka && \
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

Add the following item to the replicationEndpoints section in the `config.json`.
It is the location name we will transition our data to.

```
{
    "site": "aws-location",
    "type": "aws_s3"
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
    "s3c-location": {
        "type": "aws_s3",
        "legacyAwsBehavior": true,
        "objectId": "1b1d9226-a694-11eb-bc21-baec55d199cd",
        "details": {
            "awsEndpoint": "<RING_S3C_ENDPOINT>",
            "bucketName": "<RING_S3C_VERSIONED_BUCKET>",
            "bucketMatch": false,
            "credentialsProfile": "<RING_S3C_PROFILE (from ~/.aws/credentials)>",
            "serverSideEncryption": true,
            "https": false,
            "pathStyle": true
        }
    },
    "aws-location": {
        "type": "aws_s3",
        "legacyAwsBehavior": true,
        "objectId": "2b1d9226-a694-11eb-bc21-baec55d199cd",
        "details": {
            "awsEndpoint": "s3.amazonaws.com",
            "bucketName": "<AWS_S3_VERSIONED_BUCKET>",
            "bucketMatch": false,
            "credentialsProfile": "<AWS_S3_PROFILE (from ~/.aws/credentials)>",
            "serverSideEncryption": true
        }
    }
}
```

```
REMOTE_MANAGEMENT_DISABLE=true S3VAULT=multiple yarn start_mongo
```

## Vault

```sh
VAULT_DB_BACKEND="MONGODB" yarn start
```

Add to `./config.json`

When we create an account, it seeds the defined role into the account. 
Role is then attached with the defined trust and permission policies.

The lifecycle service user that we will create next will assume the role and
gain permission into the account. 
The `permissionPolicy.Action` is defining what API operation is the assumer allowed to perform.
The `permissionPolicy.Resource` is defining on what bucket/prefix the action can be performed.

The `permissionPolicy.Action` should be more glanular. 
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

## VaultClient

### Create a customer account with a lifecycle transition bucket


```sh
bin/vaultclient create-account --name account1 --email customer@account --port 8600
```

```sh
bin/vaultclient generate-account-access-key --name account1 --port 8600
```

```sh
aws configure --profile account1
```

```sh
aws s3api create-bucket --bucket bucket1 --create-bucket-configuration LocationConstraint=s3c-location --endpoint-url http://127.0.0.1:8000 --profile account1
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
       		"StorageClass": "aws-location"
            }],
            "ID": "456"
        }
    ]
}
```

```sh
aws s3api put-bucket-lifecycle-configuration --endpoint-url http://127.0.0.1:8000 --profile account1 --bucket bucket1 --lifecycle-configuration file://transition.json
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


## Backbeat

### Add a lifecycle conductor service user to retrieve the buckets’ account id.

This user will assume its local role with the conductor policy.

bin/ensureServiceUser apply backbeat-lifecycle-conductor -p extensions/lifecycle/conductor/policy.json --iam-endpoint http://127.0.0.1:8600 --sts-endpoint http://127.0.0.1:8800

aws configure --profile lifecycle-conductor

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

Add the following to `extensions.replication.destination.bootstrapList`
It will be used by the queue_processor to check the site.

```json
{ "site": "aws-location", "type": "aws_s3" }
```

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

### Update conf/authdata.json with account1 informations and keys.
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

### Run queue processor that includes the data mover consumer

```sh
REMOTE_MANAGEMENT_DISABLE=true yarn run queue_processor
```

### Run garbage collector

```sh
REMOTE_MANAGEMENT_DISABLE=true yarn run garbage_collector
```

## Put object for transition and check it worked

```sh
aws s3api put-object --bucket bucket1 --key test0 --body happyface.jpg --endpoint-url http://127.0.0.1:8000 --profile account1
```

### Check that the object "bucket1/test0" has been transitioned to its destination i.e. AWS S3

```sh
aws s3api head-object --bucket <AWS_S3_VERSIONED_BUCKET> --key bucket1/test0 --profile <AWS_S3_PROFILE>
```

### Check that the object "bucket1/test0" in the Ring S3C bucket has been garbage collected (deleted)

```sh
aws s3api list-object-versions --bucket <RING_S3C_VERSIONED_BUCKET> --endpoint-url http://10.100.5.232:8000 --profile <RING_S3C_PROFILE>
```
