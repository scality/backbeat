const arsenal = require('arsenal');
const LogConsumer = arsenal.storage.metadata.mongoclient.LogConsumer;
const LogReader = require('./LogReader');

class MongoLogReader extends LogReader {
    constructor(params) {
        const { zkClient, kafkaConfig, zkConfig, mongoConfig,
                logger, extensions, metricsProducer, metricsHandler } = params;
        logger.info('initializing mongo log reader',
            { method: 'MongoLogReader.constructor',
                mongoConfig });
        const logConsumer = new LogConsumer(mongoConfig, logger);
        super({ zkClient, kafkaConfig, zkConfig, logConsumer,
                logId: `mongo_${mongoConfig.logName}`, logger, extensions,
                metricsProducer, metricsHandler });
        this._mongoConfig = mongoConfig;
    }

    /**
    * start up MongoDB connection
    * @param {object} done callback function
    * @return {undefined}
    */
    setup(done) {
        this.logConsumer.connectMongo(err => {
            if (err) {
                this.log.error('error opening record log', {
                    method: 'MongoLogReader.connectMongo',
                    dmdConfig: this._mongoConfig,
                });
                return done(err);
            }
            return super.setup(done);
        });
    }

    /**
     * Get mongo log info
     * @return {object} logName
     */
    getLogInfo() {
        return { logName: this._mongoConfig.logName };
    }

    getMetricLabels() {
        return {
            logName: 'mongo-log',
            logId: this.logId,
        };
    }
}

module.exports = MongoLogReader;
