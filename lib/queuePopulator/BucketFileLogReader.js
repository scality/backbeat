const arsenal = require('arsenal');
const MetadataFileClient = arsenal.storage.metadata.MetadataFileClient;

const LogReader = require('./LogReader');

class BucketFileLogReader extends LogReader {
    constructor(params) {
        const { zkClient, zkConfig, dmdConfig, logger,
            extensions, metricsProducer } = params;
        super({ zkClient, zkConfig, logConsumer: null,
            logId: `bucketFile_${dmdConfig.logName}`, logger, extensions,
            metricsProducer });

        this._dmdConfig = dmdConfig;
        this._log = logger;
        this._log.info('initializing bucketfile log reader', {
            method: 'BucketFileLogReader.constructor',
            dmdConfig,
        });

        this._mdClient = new MetadataFileClient({
            host: dmdConfig.host,
            port: dmdConfig.port,
        });
    }

    setup(done) {
        const { logName } = this._dmdConfig;
        this._mdClient.openRecordLog({ logName }, (err, logProxy) => {
            if (err) {
                this._log.error('error opening record log', {
                    method: 'BucketFileLogReader.constructor',
                    dmdConfig: this.dmdConfig,
                });
                return done(err);
            }
            this.setLogConsumer(logProxy);
            return super.setup(done);
        });
    }

    getLogInfo() {
        return { logName: this._dmdConfig.logName };
    }
}

module.exports = BucketFileLogReader;
