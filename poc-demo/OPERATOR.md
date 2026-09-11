<!-- markdownlint-disable MD013 -->

# The delivery pool, as Federation runs it

For whoever writes the Ansible role and for whoever is on call the night it
runs. One playbook, `run.yml`, applies every change to the bucket
notification delivery pool: the migration off the per-destination queue
processors, a workgroup layout change, a version bump, a rollback. The run
is always the same three things in the same order, and there is nothing
between them.

```text
1  render the configuration on every host
2  stop every container of the old shape
3  start every container of the new shape
```

No seeding command, no drain, no barrier, no verify step, no manual pause
for an operator to type something. That is the whole point of the design:
Ansible can express stop, template, start, and a run that needs a human in
the middle is a run that half-applies across a fleet.

## What the run renders

One backbeat configuration file per host, with the notification extension's
`deliveryPool` block filled in:

```json
{
  "extensions": {
    "notification": {
      "topic": "backbeat-bucket-notification",
      "queueProcessor": { "groupId": "<the processor group base>" },
      "deliveryPool": {
        "enabled": true,
        "source": "internal",
        "topic": "backbeat-bucket-notification",
        "groupId": "bucket-notification-delivery-group",
        "seedOnStart": true,
        "deliveryTimeoutMs": 30000,
        "concurrency": 1000,
        "maxQueued": 1000,
        "workgroups": {
          "zookeeperPath": "/notification/delivery-workgroups",
          "cachePath": "/var/lib/backbeat/delivery-workgroups.json"
        }
      }
    }
  }
}
```

Four things the role has to get right, and they are the only four:

- **`queueProcessor.groupId` stays**, even after the processors are gone.
  The first generation seeds itself from `<groupId>-<destinationId>`, one
  group per destination, so the run cannot name them if the block is
  removed. Keep it for the life of the deployment; it costs nothing.
- **`source` is `internal`**. The workers read the topic the populator
  already writes to. The populator is not touched by any of this, and its
  container is not part of the run.
- **`workgroups.zookeeperPath`** is the one node that decides the shape.
  The run writes it (see "a layout change" below) and every worker reads it.
- **`DELIVERY_POOL_WORKGROUP_ID`** in each container's environment names the
  workgroup that container serves. It wins over `workgroups.id` in the file,
  so one rendered file can serve every container on a host.

`cachePath` must be writable and must differ per container when several
workers share a host; it is the copy a worker falls back to when ZooKeeper
cannot be read at start.

## What the run stops, and what it starts

**The migration.** Stops every `queue-processor` container, one per
destination, on every host. Starts one `delivery-worker` container per
workgroup, with `DELIVERY_POOL_WORKGROUP_ID` set. The run finishes the stop
phase across the whole fleet before it starts anything, and that ordering is
not cosmetic: two generations delivering the same destination at the same
time is the one thing that reorders a customer's events. The POC measured
4197 same-key inversions when two generations were allowed to overlap, and
zero when they were not. The pause below is the price of that zero.

**A version bump or a rollback.** Stops the worker containers, starts them
on the new image. The workgroups document does not change, the generation
does not change, the consumer groups do not change, and every group already
has committed offsets, so no seeding happens at all.

**A layout change.** See below: the document is rewritten between the stop
and the start, and the generation in it goes up by one.

## The self-seed

A consumer group with no committed offsets starts at the oldest record the
topic still retains. On today's topic that is hours of events to every
customer, so a new group must be told where to begin. The worker does it
itself, before it subscribes:

1. It reads the workgroups document and works out its group,
   `<deliveryPool.groupId>-<workgroup>-gen<generation>`.
2. It asks the broker what that group has committed. If every partition has
   an offset, it does nothing and joins. This is the normal case for a
   restart, a rolling replacement, a rollback, and for an operator who
   seeded ahead with the CLI.
3. If any partition has none, it creates an ephemeral node
   `<zookeeperPath>/seed-locks/gen<N>`. Exactly one worker of the generation
   wins it. The others see it, log that they are waiting, and poll for their
   own offsets to appear.
4. The winner archives the document it is seeding at
   `<zookeeperPath>/history/gen<N>`, for whichever generation replaces it,
   then seeds **every** group of the document, not just its own: per
   partition, the lowest committed offset over the groups each workgroup
   inherits from, so nothing is skipped, and a watermark per destination at
   that destination's own previous offset, so nothing already delivered is
   sent again. Generation 1 inherits from the per-destination processor
   groups; a later generation inherits from the previous generation's
   groups, named by the `previousGroups` the document carries or by that
   same archive, one generation back.
5. It reads every group back to check the offsets took, writes the
   watermarks to `<zookeeperPath>/watermarks/gen<N>`, releases the lock, and
   joins.
6. Every worker, winner or waiter, then makes the same final check: a group
   still missing an offset on any partition refuses to start, with the
   message it has always had, naming the partitions and the CLI.

The lock is ephemeral, so a worker that dies holding it releases it when its
ZooKeeper session expires. A worker that waits longer than
`seedOnStartTimeoutMs` (five minutes by default) stops waiting and falls
through to that final check, which fails the start. Nothing half-seeds: the
watermarks node is written last, after every group is committed and read
back, so if it is there the seeding completed.

**The CLI is still there.** `bin/notificationDeliverySeed.js
seed-from-processors --generation N` and `seed-from-generation --from N --to
M` do exactly what the worker does, from a shell, against stopped
containers. It refuses to seed a group that still has live members, and so
does the worker's copy, so run it after the stop phase and not before. Run it when you want to see the offset table before committing
to a run, or when you would rather the seeding not be part of the start.
With `seedOnStart: false` it becomes required again, and the run needs a
step between its stop and its start.

## What to watch during the swap

Grafana, dashboard "BNaaS delivery pool":

| Panel | What it should do |
| --- | --- |
| Delivered per second, by workgroup | drops to zero at the stop, comes back within the pause |
| Lag per workgroup consumer group, by generation | the old generation's series ends, the new one appears and falls |
| Workgroups generation per worker | every worker on the new generation, none left behind |
| Records skipped under a watermark | a burst right after the start, then nothing |
| Drops per second, by reason | flat; a step here is a destination problem, not a swap problem |
| Delivery workers up | back to the expected count |

ZooKeeper, under `<zookeeperPath>`: `watermarks/gen<N>` appears during the
start, `history/gen<N>` beside it, and `seed-locks/` is empty again.

The worker log line to grep for is `seeded itself`. Exactly one worker of
the generation prints it, with the generation in the entry; every other
worker prints that it is waiting, then joins.

## What the pause and the duplicates cost

Measured on the POC rig, single broker, three to five destinations, traffic
flowing throughout:

| Change | Pause, stop to first delivery |
| --- | --- |
| Migration off the processors (act 04) | 21 s |
| Layout change, pin a destination (act 06) | 19 s |
| Reshard, two workgroups to three (act 06) | 19 s |

The pause is the container stop and start, the group join (the consumer
session timeout, 45 s by default, is the ceiling) and about a second of
seeding. On a real fleet the container start dominates.

Duplicates are the one thing a swap costs, and they are bounded and
explained. The watermark stands at the previous owner's **committed**
offset. A consumer commits a partition contiguously, up to the oldest record
still in flight, so whatever was delivered after the last commit is above
the watermark and is delivered again by the next generation. That is
at-least-once, the same window a `kill -9` of any consumer costs. Measured:
2 duplicates across the migration in act 04's reference run, and 573 across
a generation stop in act 06's, where one slow object key held a whole
partition's committed offset back while hundreds of later records were
delivered. Nothing was ever lost, and no duplicate came from the seeding
itself.

Loss is impossible by construction: the new group starts at the lowest
offset of the groups it inherits from, so every record was either delivered
by the old generation or is read by the new one.

## A layout change

The variant of the same run. Between the stop and the start, the run writes
the new document to `<zookeeperPath>`:

```json
{
  "configVersion": 1,
  "generation": 3,
  "topic": "backbeat-bucket-notification",
  "workgroups": [
    { "id": "wg-a", "rule": { "type": "hashmod", "modulo": 4, "remainders": [0, 1] } },
    { "id": "wg-b", "rule": { "type": "hashmod", "modulo": 4, "remainders": [2] } },
    { "id": "wg-c", "rule": { "type": "hashmod", "modulo": 4, "remainders": [3] } },
    { "id": "wg-pin", "rule": { "type": "static", "destinationIds": ["noisy-tenant"] } }
  ],
  "previousGroups": [
    "bucket-notification-delivery-group-wg-a-gen2",
    "bucket-notification-delivery-group-wg-b-gen2",
    "bucket-notification-delivery-group-wg-pin-gen2"
  ]
}
```

Rules the document must satisfy, all checked before a worker will run on it:
the generation goes up, every hashmod workgroup shares one modulo, the
remainders cover the whole modulo with none claimed twice, at least one
hashmod workgroup exists so an unknown destination still has an owner, and
no destination is pinned by two workgroups. `previousGroups` is what lets
the seeding name the groups it inherits from; without it the seeding falls
back to the lowest offset over every previous group, which is safe but
delivers more twice.

A new destination is **not** a layout change. It hashes into a workgroup by
name and starts being delivered with no document change, no new generation
and no restart. Only the number of auto workgroups, or a pin, is a
generation.

## A canary

A workgroup is the unit of blast radius, so it is also the unit of a canary.
Give the destination you want to watch its own workgroup with a static rule,
run the layout change, and that destination now has its own consumer group,
its own containers and its own panels. Roll the new image onto **those**
containers only, watch their delivered and drop series for a day, and roll
the rest of the fleet after. A version bump does not touch the document, so
the canary and the fleet can sit on different images for as long as you
like.

`deliveryPool.workgroups.generation` is the guard rail next to it, not the
lever. A container whose rendered file pins a generation refuses to start on
any other one, which is how you stop a host that Ansible missed from
quietly joining the wrong generation's group. Pin it on the workers you are
holding back deliberately; leave it out everywhere else, or the next layout
change needs a config edit as well as a document write.

## When something goes wrong

| Failure | What you see | What to do |
| --- | --- | --- |
| **Populator crash** | no new records on the topic; every workgroup's lag falls to zero and stays there; delivered per second goes to zero everywhere at once | restart the populator container. It keeps its log offset in ZooKeeper under the populator path and resumes from it. Nothing about the pool is involved, and no worker needs restarting: they are idle, not broken. Delivered-per-second going to zero on **every** workgroup at once is the signature; one workgroup going quiet is a worker problem. |
| **Worker crash** | that workgroup's delivered counter stops, its lag climbs, the others are unaffected; the supervisor restarts the container | let it restart. Its group already has offsets, so it re-seeds nothing and rejoins where it was. Expect its uncommitted window to be delivered twice, at-least-once, bounded by the commit interval. If it restarts in a loop, read the log: a refusal to start names the partitions with no committed offset, and that is a seeding problem, not a crash. The worse case is a worker that is **up**, holds its partitions, answers liveness 200 and delivers nothing. Watch the delivered counter, not the lag; both workgroups read the whole topic, so their raw lag looks the same either way. The cure is a restart of that one container. |
| **Dead destination** | `Drops per second, by reason` steps up for that target: `producer_error` at once for a refused or unroutable endpoint, `delivery_error` after the delivery deadline for one that accepts and never acknowledges | nothing to do in the pool. The drop is counted, bounded by `deliveryTimeoutMs` (30 s) and confined to that destination. On today's shared topic the partitions' committed offsets hold for about one deadline while the dead records ahead of them drain, then the lag falls back; that is expected and it is bounded by the deadline, not by the endpoint. Fix the endpoint or remove the destination from the bucket's configuration. Do not restart workers: it changes nothing and costs a duplicate window. |
| **Broker restart** | rebalances in the worker logs, a lag spike, delivery resumes | let it happen. Offsets live on the broker, so nothing is re-seeded and nothing is lost. If a worker comes back and does not deliver, it is the start-up wedge: restart that container. Do **not** run the seeding CLI after a broker restart; the groups have their offsets and a seeding would only move them backwards. |
| **Bad release** | whatever the release broke, on the new image | roll the image back and run the playbook again. The document, the generation and the groups are untouched by an image change, so the run is stop and start, every group already has offsets, and nothing is seeded. This is why a rollback is cheap: it costs one pause and one uncommitted window, and no offset arithmetic. Roll the **document** back only if the release changed it, and then it is a layout change with a new generation, not a rollback. |
| **Unparseable bucket configuration** | `Records skipped, by reason` climbs on `no_config`, or `Drops per second, by reason` on `config_error` or `parse_error`; the records are committed, not stuck | three different things, and the counter tells them apart. `no_config`: the bucket had no notification configuration when the record was looked up. The worker reads it again on a bounded backoff first, because the store can be catching up behind the populator, and only then commits the record and warns, once per bucket. `config_error`: the lookup itself failed. `parse_error`: the record on the topic was not JSON. All three advance the offset, so one bad bucket cannot stall a partition or the destinations that share it, and all three are counted, so the loss is visible. Fix the bucket's configuration; the events published while it was broken are gone and the counter says how many. A steady `no_config` rate with no broken bucket means the populator is publishing for buckets the configuration store does not have, which is a store problem, not a pool one. |

Two rules that apply to all six. First, **never run the seeding CLI against
a group that has live members**: it refuses, and the refusal names the
group, but a group whose members are half-stopped can be seeded backwards.
Stop the containers first, always. Second, **a refusal to start is not a
crash loop to be worked around**. The message names the partitions with no
committed offset, and joining anyway would replay the topic to every
customer on it.

## The evidence behind this file

Every number here comes from the POC rig on this branch. `poc-demo/PLAYBOOK.md`
has the act-by-act detail and the dated runs; `poc-demo/HANDOVER.md` has the
decisions and what is still open. The self-seeding is proved against a real
broker and a real ZooKeeper by
`tests/functional/deliverypool/internalTopic.js` ("delivery workers that seed
themselves at start"), and end to end by acts 04 and 06 of the demo suite.
