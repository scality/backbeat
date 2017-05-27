# backbeat

![backbeat logo](res/backbeat-logo.png)

[![Circle CI](http://ci.ironmann.io/gh/scality/backbeat.svg?style=svg&circle-token=32e5dfd968e673450c44f0a255d1a812bae9b00c)](http://ci.ironmann.io/gh/scality/backbeat)

## Overview

Backbeat is the core engine with which we can achieve Object Lifecycle
Management, Cross Region Replication(active/active, active/passive), Utapi
metrics and Garbage Collection (potentially).

## FEATURE GOALS

* **REP.1:** (Active/Passive) Async Replication of objects at a geo site

    Typical workflow - Objects' metadata are fetched from MD log and queued in
    Kafka. Backbeat processes this queue and manages the transmission of
    objects to remote sites processing the records in FIFO.

* **REP.2:** (Active/Active) Async two-way Replication of objects on multiple sites

* **ILM.1:** Object Lifecycle Management (tiering/expiring)

    Configuration to be applied on new objects but also possibly on
    existing objects in case of policy change. Typical workflow - From
    time to time, we examine bucket metadata to see if there is a
    lifecycle policy and all objects' metadata to see if there is an
    applicable tag. If the policy (and tag) is applicable to an object, we
    pull the entries regarding the object from the log and the appropriate
    task is queued in Backbeat. Backbeat then processes this queue to
    eventually store the objects in a secondary tier.

* **UTAPI.3:** Store metrics (Utapi publishes to Backbeat)

    S3 uses Utapi to publish metrics, which will queue the metric action to
    Backbeat and backbeat is responsible for storing this metric agnostic
    to Utapi/S3. Backbeat will batch process these events and sync to
    Redis to persist in regular intervals (every 5 minutes). This makes
    sure we never lose events.

* **GC:** Perform some sort of GC for S3 to take care of orphans in data (and
    perhaps on cloud backends such as initiate MPU orphans on AWS S3)

## Design

Please refer to the ****[Design document](/DESIGN.md)****
