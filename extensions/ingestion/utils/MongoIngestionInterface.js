const arsenal = require('arsenal');
const Logger = require('werelogs').Logger;

const { MongoClientInterface } = arsenal.storage.metadata.mongoclient;
const errors = arsenal.errors;

const USERSBUCKET = arsenal.constants.usersBucket;
const PENSIEVE = 'PENSIEVE';
const METASTORE = '__metastore';

class MongoIngestionInterface extends MongoClientInterface {
    constructor(params) {
        super(Object.assign({}, params, {
            logger: new Logger('Backbeat:MongoIngestionInterface'),
        }));
    }

    getIngestionBuckets(callback) {
        const m = this.getCollection(METASTORE);
        m.find({
            '_id': {
                $nin: [PENSIEVE, USERSBUCKET],
            },
            'value.ingestion': {
                $type: 'object',
            },
        }).project({
            'value.name': 1,
            'value.ingestion': 1,
            'value.locationConstraint': 1,
        }).toArray((err, doc) => {
            if (err) {
                this.logger.error(
                    'getIngestionBuckets: error getting ingestion buckets',
                    { error: err.message });
                return callback(errors.InternalError);
            }
            return callback(null, doc.map(i => i.value));
        });
    }
}

module.exports = MongoIngestionInterface;
