# Site-Level Cross-Region Replication

## OVERVIEW

Site-level cross-region replication (CRR) is a simplified replication
setup targeted to disaster recovery. It's also been called "echo mode,"
but the preferred term is CRR.

When site-level replication is enabled, any newly-created bucket on the
source is automatically configured for replication, freeing the operator
from having to set up replication, roles and policies manually or
through a helper script. Bucket contents are replicated to remote sites
in a bucket of the same name, with the owner being the same owner-name
as the source bucket owner (Despite the same name, this describes a
different account, because the source and destination will have discrete
identity management systems).

## CONFIGURATION

To enable CRR for a destination, set `echo: true` in Federation's Backbeat
configuration section (**env_replication_endpoints** in
`env/client-template/group_vars/all`) before deploying.

Use the same Vault administrative credentials on both source and
target sites: (`env/client-template/admin-clientprofile/admin1.json`),
because Backbeat uses the same credentials to create source and target
accounts.

## WORKFLOW

When a bucket is created on the source:

* The queue populator creates an entry in Kafka for bucket creation
  when the batch is processed (normally about every 5 seconds).
* If `echo:` has been set true (CRR is enabled), the CRR queue processor
  fetches this entry, replicating the bucket on the target for CRR
  between those buckets. Otherwise, the queue processor skips the entry.
* Source account attributes (name, email) are retrieved from Vault
  using the configured Vault administrative credentials.
* A new account is created on the target with the same name and email
  as the source bucket owner (if it does not exist already), using the
  configured Vault administrative credentials.
* The first time Backbeat accesses these accounts, a new access/secret
  key pair is created for the source and target accounts. Afterwards,
  Backbeat uses cached credentials that are kept in process memory
  and not stored on disk.
* If a bucket does not already exist on the target, one is created with
  the same name as the source bucket (using the previously created
  account access key).
* New roles are created on the source and destination to support CRR
  actions through Backbeat service.
* New policies are created on the source and destination to allow CRR
  to execute the following actions on behalf of the user:
  * `ListBucket`, `GetReplicationConfiguration` on the source bucket.
  * `GetObjectVersion`, `GetObjectVersionAcl` on that bucket's source
    objects.
  * `ReplicateObject`, `ReplicateDelete` on the destination bucket's
    target objects.
* The new policies are attached to the new source and destination
  roles.
* Versioning is enabled on the source and destination buckets.
* Replication is enabled on the source bucket.

From now on, every object created on the source will have a new
version that will be replicated on the target, using roles and
temporary credentials as for normal CRR mode.

**Note:** For objects to replicate automatically, wait for all the
above steps to execute before writing new objects on the source bucket.
To determine when new objects written on the source have CRR enabled,
poll the source bucket for its replication configuration until one is
returned. (for example, with aws CLI:
`aws s3api get-bucket-replication`).

## FAILOVER SCENARIOS

If CRR is enabled on the source, it's best to enable it on the
target as well, pointing to the source as CRR target. This enables
automatic replication of objects written on the secondary site to the
primary site. This is useful in failover scenarios: when the secondary
site becomes active and receives writes, all updates are replicated to
the primary site and not lost or kept only on the secondary site when
the primary site is recovered.

However, to avoid accumulating updates to Kafka (and eventually
losing them to pruning or disk overflow) during primary-site
failover, it is important to **stop the Backbeat container on the
secondary site**, until the primary site is back up, so that updates
can be pushed to Kafka and processed in a timely fashion.

After Backbeat is started on the secondary site, a bucket creation
entry processed by Backbeat CRR for syncing to the primary site
goes to the secondary site through the same processes as normal CRR.
In other words, replication is enabled on that bucket, and new roles
and policies are created and attached. Each bucket is then accessible
to both primary- and secondary-site Backbeat processes for their
respective CRR actions. No new bucket is created on the source, which
already exists, but a new resource policy is attached to allow Backbeat
on the DR site to write new object versions.
