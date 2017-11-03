# Site-level Cross-Region Replication

## OVERVIEW

The site-level Cross-Region Replication enables a simplified
replication setup targeted for disaster recovery. It's also known as
"echo" mode.

When site-level replication is enabled, it triggers the automatic
configuration of any newly-created bucket on the source for CRR,
freeing the operator from having to setup replication, roles and
policies manually or through a helper script. The bucket contents will
be replicated to remote sites in a bucket that has the same name, of
which the owner has the same name than the source bucket owner (but a
different account because identity management is separate on source
and destination).

## CONFIGURATION

To enable echo mode for a destination, one has to set `echo: true` in
the backbeat configuration in Federation before the deployment
(section **env_replication_endpoints** in
`env/client-template/group_vars/all`).

Make sure you use the same Vault administrative credentials on both
source and target sites
(`env/client-template/admin-clientprofile/admin1.json`), as backbeat
will use the same credentials to create accounts on source and target.

## WORKFLOW

When a bucket is created on the source, the following happens:

* The queue populator creates an entry in kafka for the bucket
  creation when the batch is processed (normally every \~5 seconds)
* The CRR queue processor fetches that entry and proceeds with the
  following actions to replicate the bucket on the target and allow
  CRR to happen between those buckets, if “echo” mode is enabled,
  otherwise skips the entry.
* Source account attributes (name, email) are retrieved from Vault
  using the configured administrative credentials for Vault.
* A new account is created on the target with the same name and email
  as the source bucket owner if it does not exist already, using the
  configured administrative credentials for Vault.
* A new access/secret key pair is created for the source account and
  one for the target account, only the first time the backbeat process
  needs access to these accounts after it started, afterwards it uses
  the cached credentials (credentials are kept in the process memory
  but not stored on disk).
* A bucket is created on the target with the same name as the source
  bucket if it does not exist already, using the account access key
  previously created.
* New roles are created on the source and destination to support CRR
  actions through backbeat service.
* New policies are created on the source and destination to allow CRR
  to execute the following actions on behalf of the user:
  * `ListBucket`, `GetReplicationConfiguration` on source bucket
  * `GetObjectVersion`, `GetObjectVersionAcl` on source objects of that
    bucket
  * `ReplicateObject`, `ReplicateDelete` on target objects of the
    destination bucket
* The new policies are attached to the new roles respectively on the
  source and destination
* Versioning is enabled on the source and destination buckets
* Replication is enabled on the source bucket

From now on, every object created on the source will have a new
version that will be replicated on the target, using the role and
temporary credentials as for normal CRR mode.

Note that one has to wait until all the above steps are executed
before writing new objects on the source bucket to see them replicated
automatically. In order to know when new objects written on the source
will have CRR enabled, it’s enough to poll the source bucket for its
replication configuration until one is returned. (e.g. with aws CLI
`aws s3api get-bucket-replication`).

## FAIL-OVER SCENARIOS

If echo mode is enabled on the source, we recommend to also enable it
on the target, pointing to the source as CRR target, to have automatic
replication of objects written on the secondary site to the primary
site. This is useful in failover scenarios so that when the secondary
site becomes active and receives writes, when the primary site is
recovered, all updates will eventually be replicated again to the
primary site and not lost or kept on the secondary site only.

However, to avoid accumulating updates to Kafka during failover in
case the primary site is down, and eventually lose them because of
pruning or running out of disk space, one should **stop the backbeat
container on the secondary site**, until the primary site is back up,
so that updates can be pushed to Kafka and processed in a timely
fashion.

After backbeat is started on the secondary site, when a bucket
creation entry is processed by backbeat CRR to be synced to the
primary site, it will go through the same processing as normal CRR
to the secondary site, i.e. replication will be enabled on that
bucket, new roles and policies will be created and attached
together. Each bucket will then be accessible by both primary and
secondary site backbeat processes for their respective CRR actions. No
new bucket will be created on the source as it shall already exist,
but a new resource policy will be attached to allow backbeat on the DR
site to write new object versions.
