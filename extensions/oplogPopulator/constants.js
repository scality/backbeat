const constants = {
    bucketMetastore: '__metastore',
    defaultConnectorName: 'source-connector',
    // Max length in a pipeline is equal to the MongoDB BSON max document size,
    // so 16MB. To allow for other parameters in the pipeline, we round the max
    // to 16 MB (16777216B) / 64 (max length of a bucket name) ~= 260000
    maxBucketsPerConnector: 260000,
    mongodbVersionWithImmutablePipelines: '6.0.0',
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
        // Kafka message key config
        // The message key is set to only contain the bucket where the event happend.
        // This will make events of the same bucket always land in the same partition
        // as they will have the same key
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
                name: 'fullDocument',
                type: [{
                   type: 'record',
                   name: 'fullDocumentRecord',
                   fields: [{
                        name: 'value',
                        type: [{
                            type: 'record',
                            name: 'valueRecord',
                            fields: [{
                                name: 'key',
                                type: ['string', 'null'],
                            }],
                        }, 'null'],
                   }],
                }, 'null'],
            }],
        }),
    },
};

module.exports = constants;
