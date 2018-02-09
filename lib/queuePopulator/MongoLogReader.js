const arsenal = require('arsenal');
const LogConsumer = arsenal.storage.metadata.mongoclient.LogConsumer;
const LogReader = require('./LogReader');

class MongoLogReader extends LogReader {
    constructor(params) {
        const { zkClient, zkConfig, mongoConfig,
                logger, extensions } = params;
        logger.info('initializing mongo log reader',
            { method: 'MongoLogReader.constructor',
                mongoConfig });
        const logConsumer = new LogConsumer(mongoConfig, logger);
        super({ zkClient, zkConfig, logConsumer,
                logId: `mongo_${mongoConfig.logName}`, logger, extensions });
        this._mongoConfig = mongoConfig;
    }

    /**
    * start up MongoDB connection
    */
    setup(done) {
        this.logConsumer.connectMongo(err => {
            if (err) {
                this._log.error('error opening record log', {
                    method: 'BucketFileLogReader.constructor',
                    dmdConfig: this._mongoConfig,
                });
                return done(err);
            }
            return super.setup(done);
        });
    }

    getLogInfo() {
        return { logName: this._mongoConfig.logName };
    }
}

module.exports = MongoLogReader;
