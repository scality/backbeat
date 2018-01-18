const arsenal = require('arsenal');
const LogConsumer = arsenal.storage.metadata.mongoclient.LogConsumer;

const LogReader = require('./LogReader');

class MongoLogReader extends LogReader {
    constructor(params) {
        const { zkClient, zkConfig, mongoConfig,
                logger, extensions } = params;
        const { host, port } = mongoConfig;
        logger.info('initializing mongo log reader',
            { method: 'MongoLogReader.constructor',
                mongoConfig });
        const logConsumer = new LogConsumer({ host, port, logger });
        super({ zkClient, zkConfig, logConsumer,
                logId: `mongo_${mongoConfig.logName}`, logger, extensions });
    }

    getLogInfo() {
        return { logName: this._mongoConfig.logName };
    }
}

module.exports = MongoLogReader;
