const arsenal = require('arsenal');
const MetadataFileClient = arsenal.storage.metadata.file.MetadataFileClient;

const LogReader = require('./LogReader');

class BucketFileLogReader extends LogReader {
    constructor(params) {
        const { zkClient, kafkaConfig, dmdConfig, logger,
            extensions, extensionNames, metricsProducer, metricsHandler } = params;
        super({ zkClient, kafkaConfig, logConsumer: null,
            logId: `bucketFile_${dmdConfig.logName}`, logger, extensions,
            metricsProducer, metricsHandler });

        this._dmdConfig = dmdConfig;
        this._log = logger;
        this._extensionNames = extensionNames;
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

    getMetricLabels() {
        return {
            origin: this._extensionNames,
            logName: 'bucket-file',
            logId: this._dmdConfig.logName,
        };
    }
}

module.exports = BucketFileLogReader;
