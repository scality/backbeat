const constants = {
    bucketMetastore: '__metastore',
    defaultConnectorConfig: {
        'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
        'change.stream.full.document': 'updateLookup',
        'pipeline': '[]',
        'collection': '',
    }
};

module.exports = constants;
