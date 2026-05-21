const transformObjectKeyClass =
    'com.scality.kafka.connect.transforms.TransformObjectKey';

const defaultConnectorConfig = {
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
    // Kafka message key config (legacy).
    // The key schema projects {ns.coll, fullDocument.value.key}. fullDocument
    // is null on update/delete events (BB-355 removed updateLookup), so for
    // those op types the key collapses to hash({ns, null}) and lands on a
    // different partition than insert events for the same S3 object. See
    // BB-768. This is overridden by smtKeyConfig when the oplogPopulator
    // 'transformObjectKey' flag is enabled (and the TransformObjectKey SMT
    // is available in the Kafka Connect plugin path).
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
};

// Key-config overrides applied on top of defaultConnectorConfig when the
// 'transformObjectKey' flag is set. Projects documentKey._id (always
// populated on every change-stream event) and adds the TransformObjectKey
// SMT, which strips the arsenal master/version encoding so master and all
// versions of the same S3 object hash to the same partition. The SMT
// rewrites the projected Struct into a plain string, serialised by
// key.converter=StringConverter.
//
// The keys here that are absent from defaultConnectorConfig (transforms*,
// key.converter) are the ones to drop when reverting a connector to the
// legacy schema; output.schema.key is restored from defaultConnectorConfig.
const smtKeyConfig = {
    'output.schema.key': JSON.stringify({
        type: 'record',
        name: 'keySchema',
        fields: [{
            name: 'documentKey',
            type: [{
                type: 'record',
                name: 'documentKeyRecord',
                fields: [{
                    name: '_id',
                    type: ['string', 'null'],
                }],
            }, 'null'],
        }],
    }),
    'key.converter': 'org.apache.kafka.connect.storage.StringConverter',
    'transforms': 'stripObjectKey',
    'transforms.stripObjectKey.type': transformObjectKeyClass,
};

const constants = {
    bucketMetastore: '__metastore',
    defaultConnectorName: 'source-connector',
    // Max length in a pipeline is equal to the MongoDB BSON max document size,
    // so 16MB. To allow for other parameters in the pipeline, we round the max
    // to 16 MB (16777216B) / 64 (max length of a bucket name) ~= 260000
    maxBucketsPerConnector: 260000,
    mongodbVersionWithImmutablePipelines: '6.0.0',
    wildCardForAllBuckets: '*',
    connectorUpdatedEvent: 'connector-updated',
    bucketRemovedFromConnectorEvent: 'bucket-removed',
    connectorsReconciledEvent: 'connectors-reconciled',
    transformObjectKeyClass,
    defaultConnectorConfig,
    smtKeyConfig,
};

module.exports = constants;
