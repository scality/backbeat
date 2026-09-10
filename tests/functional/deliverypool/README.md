# Delivery pool functional suites

Each suite needs a real broker on `localhost:9092`, and the workgroup suites
also need zookeeper on `localhost:2181`. Every run scopes its topics, consumer
groups, znodes and metric labels to a run id, so runs never collide. None of
them is part of CI: they are long, they talk to real brokers, and they hit the
pre-existing consumer wedge of `design/06-backbeatconsumer-wedge.md`.

| Script | Suite |
| --- | --- |
| `yarn ft_test:notification:deliverypool` | one pool delivering to the destination each record names |
| `yarn ft_test:notification:workgroups` | several consumer groups over one delivery topic, slices and cutovers |
| `yarn ft_test:notification:assumedestination` | a workgroup that ignores the destination on the record and delivers everything to its own, and what it does with a topic of unaddressed records |
