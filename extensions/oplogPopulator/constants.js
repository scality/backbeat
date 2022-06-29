const constants = {
    bucketMetastore: '__metastore',
    defaultConnectorName: 'source-connector',
    defaultConnectorConfig: {
        'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
        'change.stream.full.document': 'updateLookup',
        'pipeline': '[]',
        'collection': '',
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
    extensionConfigField: {
        notification: 'notificationConfiguration',
        replication: 'replicationConfiguration',
        lifecycle: 'lifecycleConfiguration',
        ingestion: 'ingestion',
    },
};

module.exports = constants;
