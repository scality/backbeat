# Zenko Backbeat

![backbeat logo](res/backbeat-logo.png)

[![CircleCI][badgepub]](https://circleci.com/gh/scality/backbeat)
[![Scality CI][badgepriv]](http://ci.ironmann.io/gh/scality/backbeat)

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

* Using MacOS
* `brew` is installed (get it [here](https://brew.sh/))
* `node` is installed (version 6.9.5)
* `npm` is installed (version 3.10.10)
* `aws` is installed (version 1.11.1)

### Run kafka and zookeeper

#### Install kafka and zookeeper

```
brew install kafka && brew install zookeeper
```

Make sure you have `/usr/local/bin` in your `PATH` env variable (or wherever
your homebrew programs are installed):

```
echo 'export PATH="$PATH:/usr/local/bin"' >> ~/.bash_profile
```

#### Start kafka and zookeeper servers

```
mkdir ~/kafka && \
cd ~/kafka && \
curl http://apache.claz.org/kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz | tar xvz && \
sed 's/zookeeper.connect=.*/zookeeper.connect=localhost:2181\/backbeat/' \
kafka_2.11-0.11.0.0/config/server.properties > \
kafka_2.11-0.11.0.0/config/server.properties.backbeat
```

Start the zookeeper server:

```
zookeeper-server-start ~/kafka/kafka_2.11-0.11.0.0/config/zookeeper.properties
```

In a new shell, start the kafka server:

```
kafka-server-start ~/kafka/kafka_2.11-0.11.0.0/config/server.properties.backbeat
```

#### Create a zookeeper node and kafka topic

In a new shell, connect to the zookeeper server with the ZooKeeper chroot
`/backbeat` path:

```
zkCli -server localhost:2181/backbeat
```

Create the `replication-populator` node:

```
create /replication-populator my_data
```

We may leave the zookeeper server now:

```
quit
```

Create the `backbeat-replication` kafka topic:

```
kafka-topics --create \
--zookeeper localhost:2181/backbeat \
--replication-factor 1 \
--partitions 1 \
--topic backbeat-replication
```

### Run Scality Components

#### Start Vault and Scality S3 servers

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

#### Setup replication with backbeat

In a new shell, clone backbeat:

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

Output will look something like (this output is stored for reference in the file
`backbeat_user_credentials`):

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

Store the account's credentials using the "id" and "value" fields:

```
aws configure --profile backbeatuser
```

The completed prompt should look like:

```
AWS Access Key ID [None]: 8CFJQ2Z3R6LR0WTP5VDS
AWS Secret Access Key [None]: gB53GM7/LpKrm6DktUUarcAOcqHS2tvKI/=CxFxR
Default region name [None]:
Default output format [None]:
```

Set up replication on your buckets:

```
node ~/replication/backbeat/bin/replication.js setup \
--source-bucket source-bucket \
--source-profile backbeatuser \
--target-bucket target-bucket \
--target-profile backbeatuser
```

Run the backbeat queue populator:

```
npm --prefix ~/replication/backbeat run queue_populator
```

In a new shell, run the backbeat queue processor:

```
npm --prefix ~/replication/backbeat run queue_processor
```

You are now ready to put data on `source-bucket` and watch it replicate to
`target-bucket`!

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
:smiley_cat:

### More on Bucket Setup Script

An optional bucket setup script is provided to quickly create (if bucket(s) do
not already exist) and/or configure two buckets for replication. This section
will expand upon the details of the script and what it has to offer.

As a note, this script was used above to setup replication on buckets. The
example commands provided in this section **will assume you run them from the
root directory** of this Backbeat project.

To use this script, we require you to have setup one or two AWS profiles within
the `~/.aws/credentials` file. You can follow the [AWS create-access-key Guide](https://aws.amazon.com/premiumsupport/knowledge-center/create-access-key/)
to create your own AWS keys. If our Vault service is available to you, we
recommend you generate these keys from their respective server endpoints.

Here is an example of two profiles I created and added to `~/.aws/credentials`:

```
[backbeat-source]
aws_access_key_id = V28ROGMVX4C5R80TP5P3
aws_secret_access_key = zwksPAYyx1uwyGK/ezCpPi8tS4H3NYTZrslYZeAx

[backbeat-target]
aws_access_key_id = RPHPFTBTF77MNIDF1JJ9
aws_secret_access_key = rjTqIFBvNALJz=qgipmW/uU+I2BjU=o+WDcuOI22
```

Once credentials are setup, let's run the script to setup two buckets for
replication. Below, you can specify any [valid AWS bucket name](http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules)
for `source-bucket` and `destination-bucket`, and specify the profile names you
set above:

```
node bin/replication.js setup \
--source-bucket source-bucket \
--source-profile backbeat-source \
--target-bucket target-bucket \
--target-profile backbeat-target
```

If all went well, you should see a log message that says "replication setup
successful", similar to:

```
{
    "name":"BackbeatSetup",
    "time":1504041946570,
    "req_id":"4da310488c4b99ccbe00",
    "level":"info",
    "message":"replication setup successful",
    "hostname":"test-user.local",
    "pid":43849
}
```

The script has other available commands as well:

```
# To view all commands
node bin/replication.js --help

# To view the options for specific commands, for example `setup`
node bin/replication.js setup --help
```

### Structure

In our `$HOME` directory, we now have the following directories:

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
