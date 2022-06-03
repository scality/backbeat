const constants = {
    bucketMetastore: '__metastore',
    defaultConnectorName: 'source-connector',
    defaultConnectorConfig: {
        'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
        'change.stream.full.document': 'updateLookup',
        'pipeline': '[]',
        'collection': '',
    },
    extensionConfigField: {
        notification: 'notificationConfiguration',
        replication: 'replicationConfiguration',
        lifecycle: 'lifecycleConfiguration',
        ingestion: 'ingestion',
    },
};

module.exports = constants;
