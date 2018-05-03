# QUICKSTART

This guide assumes the following:

* MacOS
* `brew` is installed (get it [here](https://brew.sh/))
* `node` is installed (version 6.9.5)
* `npm` is installed (version 3.10.10)
* `aws` is installed (version 1.11.1)

## Run Kafka and ZooKeeper

### Install Kafka and ZooKeeper

```
brew install kafka && brew install zookeeper
```

Make sure `/usr/local/bin` is in your `PATH` env variable (or wherever
your homebrew programs are installed):

```
echo 'export PATH="$PATH:/usr/local/bin"' >> ~/.bash_profile
```

### Start Kafka and ZooKeeper Servers

```
mkdir ~/kafka && \
cd ~/kafka && \
curl http://apache.claz.org/kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz | tar xvz && \
sed 's/zookeeper.connect=.*/zookeeper.connect=localhost:2181\/backbeat/' \
kafka_2.11-0.11.0.0/config/server.properties > \
kafka_2.11-0.11.0.0/config/server.properties.backbeat
```

Start the ZooKeeper server:

```
zookeeper-server-start ~/kafka/kafka_2.11-0.11.0.0/config/zookeeper.properties
```

In a new shell, start the Kafka server:

```
kafka-server-start ~/kafka/kafka_2.11-0.11.0.0/config/server.properties.backbeat
```

### Create a ZooKeeper Node and a Kafka Topic

In a new shell, connect to the ZooKeeper server with the ZooKeeper chroot
`/backbeat` path:

```
zkCli -server localhost:2181/backbeat
```

Create the `replication-populator` node:

```
create /replication-populator my_data
```

Leave the ZooKeeper server:

```
quit
```

Create the `backbeat-replication` Kafka topic:

```
kafka-topics --create \
--zookeeper localhost:2181/backbeat \
--replication-factor 1 \
--partitions 1 \
--topic backbeat-replication
```

### Run Scality Components

#### Start Vault and Scality S3 Servers

Start the Vault server (this requires access to the private Vault repository):

```
git clone https://github.com/scality/Vault ~/replication/vault && \
cd ~/replication/vault && \
npm i && \
chmod 400 ./tests/utils/keyfile && \
VAULT_DB_BACKEND=MEMORY node vaultd.js
```

In a new shell, start the Scality S3 server:

```
git clone https://github.com/scality/s3 ~/replication/s3 && \
cd ~/replication/s3 && \
npm i && \
S3BACKEND=file S3VAULT=scality npm start
```

#### Set Up Replication with Backbeat

In a new shell, clone Backbeat:

```
git clone https://github.com/scality/backbeat ~/replication/backbeat && \
cd ~/replication/backbeat && \
npm i
```

Now, create an account and keys:

```
VAULTCLIENT=~/replication/backbeat/node_modules/vaultclient/bin/vaultclient && \
$VAULTCLIENT create-account \
--name backbeatuser \
--email dev@null \
--port 8600 >> backbeat_user_credentials && \
$VAULTCLIENT generate-account-access-key \
--name backbeatuser \
--port 8600 >> backbeat_user_credentials && \
cat backbeat_user_credentials
```

The output will resemble:

```
...
{
    "id": "8CFJQ2Z3R6LR0WTP5VDS",
    "value": "gB53GM7/LpKrm6DktUUarcAOcqHS2tvKI/=CxFxR",
    "createDate": "2017-08-03T00:17:57Z",
    "lastUsedDate": "2017-08-03T00:17:57Z",
    "status": "Active",
    "userId": "038628340774"
}
```

(This output is stored for reference in the
`backbeat_user_credentials` file)

Store the account's credentials using the "id" and "value" fields:

```
aws configure --profile backbeatuser
```

The completed prompt resembles:

```
AWS Access Key ID [None]: 8CFJQ2Z3R6LR0WTP5VDS
AWS Secret Access Key [None]: gB53GM7/LpKrm6DktUUarcAOcqHS2tvKI/=CxFxR
Default region name [None]:
Default output format [None]:
```

Set up replication on your buckets:

```
node ~/replication/backbeat/bin/setupReplication.js setup \
--source-bucket source-bucket \
--source-profile backbeatuser \
--target-bucket target-bucket \
--target-profile backbeatuser
```

Run Backbeat's queue populator:

```
npm --prefix ~/replication/backbeat run queue_populator
```

In a new shell, run Backbeat's queue processor:

```
npm --prefix ~/replication/backbeat run queue_processor
```

Data put on `source-bucket` now replicates to `target-bucket`.

Put an object in `source-bucket`:

```
echo 'content to be replicated' > replication_contents && \
aws s3api put-object \
--bucket source-bucket \
--key object-to-replicate \
--body replication_contents \
--endpoint http://localhost:8000 \
--profile backbeatuser
```

Check the replication status of the just-put object:

```
aws s3api head-object \
--bucket source-bucket \
--key object-to-replicate \
--endpoint http://localhost:8000 \
--profile backbeatuser
```

The object's "ReplicationStatus" will be either "PENDING", or if some time has
passed, "COMPLETED".

Check if the object has been replicated to the target bucket:

```
aws s3api head-object \
--bucket target-bucket \
--key object-to-replicate \
--endpoint http://localhost:8000 \
--profile backbeatuser
```

After some time, the object's "ReplicationStatus" will be "REPLICA".

### Structure

The `$HOME` directory now contains the following directories:

```
$HOME
├── kafka
│   └── kafka_2.11-0.11.0.0
├── replication
    ├── backbeat
    ├── s3
    └── vault
```

[badgepriv]: http://ci.ironmann.io/gh/scality/backbeat.svg?style=svg&circle-token=32e5dfd968e673450c44f0a255d1a812bae9b00c
[badgepub]: https://circleci.com/gh/scality/backbeat.svg?style=svg
