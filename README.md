# backbeat

![backbeat logo](res/backbeat-logo.png)

[![Circle CI](http://ci.ironmann.io/gh/scality/backbeat.svg?style=svg&circle-token=32e5dfd968e673450c44f0a255d1a812bae9b00c)](http://ci.ironmann.io/gh/scality/backbeat)

## OVERVIEW

Backbeat is an engine with a messaging system at its heart. The core
engine can be extended for many use cases.

## EXTENSIONS

### Asynchronous Replication

    This feature replicates objects from one S3 bucket to
    another S3 bucket in a different geological region. The extension uses
    Metadata journal as the source of truth and replicates object updates in a
    FIFO order.

## DESIGN

Please refer to the ****[Design document](/DESIGN.md)****
