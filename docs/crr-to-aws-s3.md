# CRR from CloudServer to AWS S3 - Workflow

## Installation

AWS S3 endpoint (`awsbackend` for example) will be defined as a location
constraint in the locationConstraint section of
[CloudServer](https://github.com/scality/S3). The locationConstraint config has
properties to set the bucket name, region, credentials for AWS S3.This enables
multiple backend feature in CloudServer which Backbeat will leverage to CRR to
AWS S3.

replication endpoints will be defined in the config for both Backbeat and
CloudServer

```
env_replication_endpoints:
  site: awsbackend
  type: aws_s3
```

### Operations

#### CloudServer: PUT Bucket Replication Request

- CloudServer receives the request and checks the Storage Class set in the
  request

- Check in replicationEndpoints defined in config.json
    - It should match one of the sites
    - If site matches, then it should either have type or servers property
    - If type is aws_s3, CloudServer writes this to the bucket's replication
        config property, `storageType`

#### CloudServer: PUT object requests on bucket with replication enabled

* CloudServer receives the request and checks if bucket has replication enabled

* If replication is enabled, storageType and storageClass  are added to the
  object's replicationInfo property and replicationStatus is set to PENDING.

#### Overview of CRR processing

* Queue Populator populates the metadata entry of the object in Kafka since it
  has replicationStatus `PENDING`

* QueueProcessor reads the entry from Kafka and sees that storageType is
  `aws_s3`

* QueueProcessor spawns AWSReplicationTask to use the Backbeat routes in
  CloudServer to put objects in AWS S3

#### AWS Replication Task

* Upon acquiring an entry to replicate, the task sends a GET Object ?versionId
  request to CloudServer

* The stream is piped to PUT /_/backbeat Data request with header
  `x-scal-replication-storage-type: aws_s3`

* Upon receiving 200, read `x-amz-scal-version-id` from the header and send PUT
  /_/backbeat MD request on source to update the properties replicationStatus to
  COMPLETED, x-amz-scal-version-id to the value received in the header.

#### CloudServer PUT /\_/backbeat Data for AWS route

* If the request has header `x-scal-replication-storage-type: aws_s3`, use the
  multipleDataBackend.

* MultipleDataBackend puts in AWS S3 with extra user metadata
  `x-amz-scal-replication-status: REPLICA` and stores CloudServer's versionId for
  the entry in `x-amz-scal-version-id`

* If we get 200 from AWS, CloudServer responds back with 200 to Backbeat with
  x-amz-version-id it received from AWS in the header/body

* Backbeat updates the source object's metadata to store AWS' version id and set
  replication status as `COMPLETED`

## Setting up Cross-Region Replication (CRR) to AWS

This section contains steps to set up cross-region replication from Scality S3
to Amazon Web Services S3 using the Backbeat replication engine.

### Half-Automated Example

This example demonstrates how to set up replication using a built-in command
line tool provided by Backbeat.

1. Create the target bucket:

    ```
    aws s3api create-bucket \
    --bucket <target-bucket> \
    --profile <target-profile>
    ```

2. Enable versioning on the target bucket:

    ```
    aws s3api put-bucket-versioning \
    --bucket <target-bucket> \
    --versioning-configuration Status=Enabled \
    --profile <target-profile>
    ```

3. Generate account credentials for the source site using the command:

    ```
    ansible-playbook -i env/client-template/inventory \
    tooling-playbooks/generate-account-access-key.yml
    ```

4. At the source, log in to the docker shell of a Backbeat container:

    ```
    docker exec -it <backbeat-container-name> bash
    ```

    Note that on a full deployment the container name for Backbeat would be
    'scality-backbeat-1' while with a light-local deployment the name would
    be 'scality-backbeat-127.0.0.2-1'.

5. Store the credentials generated in step 3 as the profile `<source-profile>`:

    ```
    mkdir ~/.aws
    vi ~/.aws/credentials
    ```

    The completed file should be similar to the following:

    ```
    [<source-profile>]
    aws_access_key_id = <access-key>
    aws_secret_access_key = <secret-key>
    ```

6. Set up replication on your buckets:

    ```
    node ~/replication/backbeat/bin/setupReplication.js setup \
    --source-bucket <source-bucket> \
    --source-profile <source-profile> \
    --target-bucket <target-bucket> \
    --site-name <target-site-name>
    ```

### Manual Example

This example demonstrates how to set up replication manually using the AWS cli.

1. Create source and target buckets:

    ```
    aws s3api create-bucket \
    --bucket <source-bucket> \
    --endpoint http://<source> \
    --profile <source-profile>
    ```

    ```
    aws s3api create-bucket \
    --bucket <target-bucket> \
    --profile <target-profile>
    ```

2. Enable bucket versioning on each bucket:

    ```
    aws s3api put-bucket-versioning \
    --bucket <source-bucket> \
    --versioning-configuration Status=Enabled \
    --endpoint http://<source> \
    --profile <source-profile>
    ```

    ```
    aws s3api put-bucket-versioning \
    --bucket <target-bucket> \
    --versioning-configuration Status=Enabled \
    --profile <target-profile>
    ```

    Verify that bucket versioning is enabled on each bucket:

    ```
    aws s3api get-bucket-versioning \
    --bucket <source-bucket> \
    --endpoint http://<source> \
    --profile <source-profile>
    ```

    ```
    aws s3api get-bucket-versioning \
    --bucket <target-bucket> \
    --profile <target-profile>
    ```

3. Establish a bucket policy to allow the Backbeat replication engine to
    execute the required actions. Create the file _policy.json_ with the
    following content:

    ```
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObjectVersion",
                    "s3:GetObjectVersionAcl",
                    "s3:GetObjectVersionTagging"
                ],
                "Resource": [
                    "arn:aws:s3:::<source-bucket>/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:ListBucket",
                    "s3:GetReplicationConfiguration"
                ],
                "Resource": [
                    "arn:aws:s3:::<source-bucket>"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:ReplicateObject",
                    "s3:ReplicateDelete",
                    "s3:ReplicateTags"
                ],
                "Resource": "arn:aws:s3:::<target-bucket>/*"
            }
        ]
    }
    ```

    Create the policy on the source IAM environment using the policy in
    _policy.json_:

    ```
    aws iam create-policy \
    --policy-name replication-policy \
    --policy-document file://policy.json \
    --endpoint http://<source>:8600 \
    --profile <source-profile>
    ```

    The response output should look something like:

    ```
    {
        "Policy": {
            "PolicyName": "replication-policy",
            "CreateDate": "2017-07-21T18:44:01Z",
            "AttachmentCount": 0,
            "IsAttachable": true,
            "PolicyId": "R8W4K5XV2XDF4ZLH0NW9ZPCZDEI4H5CK",
            "DefaultVersionId": "v1",
            "Path": "/",
            "Arn": "arn:aws:iam::316133440783:policy/replication-policy",
            "UpdateDate": "2017-07-21T18:44:01Z"
        }
    }
    ```

4. Create a role for the Backbeat replication engine to use.

    Create the file _trust.json_ with the following content:

    ```
    {
        "Version":"2012-10-17",
        "Statement":[
            {
                "Effect":"Allow",
                "Principal":{
                    "Service":"backbeat"
                },
                "Action":"sts:AssumeRole"
            }
        ]
    }
    ```

    Create the role on the source environment:

    ```
    aws iam create-role \
    --role-name replication-role \
    --assume-role-policy-document file://trust.json \
    --endpoint http://<source>:8600 \
    --profile <source-profile>
    ```

    Response output should be something like:

    ```
    {
        "Role": {
            "AssumeRolePolicyDocument": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "sts:AssumeRole",
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "backbeat"
                        }
                    }
                ]
            },
            "RoleId": "ZX6QI5K88ISYCNMIDZVJTBW5QE101DLN",
            "CreateDate": "2017-07-21T18:46:31Z",
            "RoleName": "replication-role",
            "Path": "/",
            "Arn": "arn:aws:iam::316133440783:role/replication-role"
        }
    }
    ```

5. Attach the policy we created to the role (note that the policy ARN is found
    in the response output in step 3):

    ```
    aws iam attach-role-policy \
    --policy-arn arn:aws:iam::316133440783:policy/replication-policy \
    --role-name replication-role \
    --endpoint http://<source>:8600 \
    --profile <source-profile>
    ```

6. Create the file _replication.json_ with the following content (note the role
    ARN is found in the response output in step 4):

    ```
    {
        "Role": "arn:aws:iam::316133440783:role/replication-role",
        "Rules": [
            {
                "Prefix": "",
                "Status": "Enabled",
                "Destination": {
                    "Bucket": "arn:aws:s3:::<target-bucket>",
                    "StorageClass": "awsbackend"
                }
            }
        ]
    }
    ```

    The above replication configuration will replicate any object put in
    `<source-bucket>` to the AWS bucket `<target-bucket>` using the credentials
    that are mapped to the site name `awsbackend`.

    Put the replication configuration on the source bucket:

    ```
    aws s3api put-bucket-replication \
    --bucket <source-bucket> \
    --replication-configuration file://replication.json \
    --endpoint http://<source> \
    --profile <source-profile>
    ```

### Test Object Replication Example

Whether you followed the half-automated or manual example to set up replication,
we are ready to put data on the source bucket and confirm that it replicates to
the target bucket.

1. Put an object on the source bucket:

    ```
    echo 'content to be replicated' > replication_contents && \
    aws s3api put-object \
    --bucket <source-bucket> \
    --key <object-key> \
    --body replication_contents \
    --endpoint http://<source> \
    --profile <source-profile>
    ```

2. Check the replication status and user metadata of our objects:

    ```
    aws s3api head-object \
    --bucket <source-bucket> \
    --key <object-key> \
    --endpoint http://<source> \
    --profile <source-profile>
    ```

    The object's ReplicationStatus will either be 'PENDING' or 'COMPLETED',
    depending on how long replication takes. When the ReplicationStatus is
    'COMPLETED', the response object Metadata 'aws-s3-version-id' value will be
    the version ID of the AWS object replica.

3. Verify that the object has been replicated to the target bucket:

    ```
    aws s3api head-object \
    --bucket <target-bucket> \
    --key <object-key> \
    --profile <target-profile>
    ```

    After some time, the object will be replicated and the response object
    Metadata will contain two value: 'scal-replication-status' (set to
    'REPLICA') and 'scal-version-id' (set to the Scality source object's version
    ID).
