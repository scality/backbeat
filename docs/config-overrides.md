# Configuration Overrides

`BACKBEAT_CONFIG_OVERRIDES` holds a JSON document applied on top of the
configuration file at startup, so that any field can be changed per process
without a new image or release.

This is a troubleshooting escape hatch, meant for support: the environment
variables named after the configuration fields, described in
[Configuration](/docs/configuration.md), remain the supported way to configure
backbeat, and should be preferred whenever one exists for the field at hand.

## Semantics

The document is applied as a [JSON Merge Patch](https://www.rfc-editor.org/rfc/rfc7386),
which is to say:

- objects are merged recursively, so only the fields mentioned are changed;
- arrays and scalars replace the value they override — an array is never merged
  element-wise, so overriding a list with a shorter one drops the extra entries;
- `null` deletes a field, restoring the default the schema defines for it.

The overrides are applied last, over the configuration file and any other
setting, so that nothing silently overrides them.

The result is validated as a whole, exactly like the configuration file: an
unknown field, a wrong type or a deleted mandatory field fails at startup,
rather than leaving a setting silently ignored. Values are coerced by the
schema, so `"250"` is accepted for a numeric field.

## Examples

Raise the log level of a single process:

```sh
BACKBEAT_CONFIG_OVERRIDES='{"log":{"logLevel":"debug"}}'
```

Set librdkafka producer parameters, whose dotted keys need no escaping, being
plain JSON object keys:

```sh
BACKBEAT_CONFIG_OVERRIDES='{"kafka":{"producerParams":{"linger.ms":10}}}'
```

Change a field of an extension, and restore another to its default:

```sh
BACKBEAT_CONFIG_OVERRIDES='{"extensions":{"lifecycle":{"conductor":{"concurrency":20}}}}'
BACKBEAT_CONFIG_OVERRIDES='{"queuePopulator":{"batchMaxRead":null}}'
```

Several changes are applied in a single document:

```sh
BACKBEAT_CONFIG_OVERRIDES='{
    "log": { "logLevel": "debug" },
    "queuePopulator": { "batchMaxRead": 250 },
    "extensions": { "gc": { "consumer": { "concurrency": 5 } } }
}'
```
