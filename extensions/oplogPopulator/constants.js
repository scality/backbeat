const { constants: arsenalConstants } = require('@scality/arsenal');

const constants = {
    bucketMetastore: '__metastore',
    defaultConnectorName: 'source-connector',
    // Max length in a pipeline is equal to the MongoDB BSON max document size,
    // so 16MB. To allow for other parameters in the pipeline, we round the max
    // to 16 MB (16777216B) / 64 (max length of a bucket name) ~= 260000
    maxBucketsPerConnector: 260000,
    mongodbVersionWithImmutablePipelines: '6.0.0',
    wildCardForAllBuckets: '*',
    // MPU shadow buckets and internal collections (__metastore, ...)
    internalCollectionsRegex: `^(${arsenalConstants.mpuBucketPrefix}|__).*`,
    // operation types carrying object metadata, the only ones consumed
    // downstream (object deletions are updates setting the deleted flag)
    hashedOperationTypes: ['insert', 'update', 'replace'],
    // kafka connect worker default, not exposed through its REST API: a
    // running connector commits its offsets within this delay
    kafkaConnectOffsetFlushIntervalMs: 60000,
    // how long to wait for a stopped connector's tasks to shut down before
    // retrying later
    stopTimeoutMs: 60000,
    stopPollIntervalMs: 1000,
    // bounds parallel kafka connect requests: there may be thousands of per-bucket
    // connectors to migrate, and some calls are expensive (offsets reads make the
    // worker read its whole offsets topic)
    kafkaConnectConcurrency: 10,
    connectorPartitionNameKey: 'offset.partition.name',
    connectorStartupTimeKey: 'startup.mode.timestamp.start.at.operation.time',
    connectorUpdatedEvent: 'connector-updated',
    bucketRemovedFromConnectorEvent: 'bucket-removed',
    connectorsReconciledEvent: 'connectors-reconciled',
    defaultConnectorConfig: {
        'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
        'pipeline': '[]',
        'collection': '',
        // If no timestamp is provided, the startup mode will be equivalent
        // to 'latest' which will pick up from the latest event in the oplog
        'startup.mode': 'timestamp',
        // JSON output converter config
        // Using a string converter to avoid getting an over-stringified
        // JSON that is returned by default
        'output.format.value': 'json',
        'value.converter.schemas.enable': false,
        'value.converter': 'org.apache.kafka.connect.storage.StringConverter',
        'output.format.key': 'schema',
        'output.schema.key': JSON.stringify({
            type: 'record',
            name: 'keySchema',
            fields: [{
                name: 'ns',
                type: [{
                        name: 'ns',
                        type: 'record',
                        fields: [{
                            name: 'coll',
                            type: ['string', 'null'],
                        }],
                    }, 'null'],
            }, {
                name: 'key',
                type: ['string', 'null'],
            }],
        }),
    },
};

module.exports = constants;
