const arsenal = require('arsenal');
const LogConsumer = arsenal.storage.metadata.bucketclient.LogConsumer;
const BucketClient = require('bucketclient').RESTClient;

const LogReader = require('./LogReader');

class RaftLogReader extends LogReader {
    constructor(params) {
        const { zkClient, kafkaConfig, bucketdConfig, httpsConfig,
                raftId, logger, extensions, metricsProducer, metricsHandler } = params;
        const { host, port } = bucketdConfig;
        logger.info('initializing raft log reader',
            { method: 'RaftLogReader.constructor',
              bucketdConfig, raftId });
        const _httpsConfig = httpsConfig || {};
        let bucketClient;
        if (bucketdConfig.transport === 'https') {
            bucketClient = new BucketClient(
                `${host}:${port}`, undefined, true,
                _httpsConfig.key, _httpsConfig.cert, _httpsConfig.ca);
        } else {
            bucketClient = new BucketClient(`${host}:${port}`);
        }
        const logConsumer = new LogConsumer({ bucketClient,
            raftSession: raftId,
            logger });
        super({ zkClient, kafkaConfig, logConsumer, logId: `raft_${raftId}`,
                logger, extensions, metricsProducer, metricsHandler });
        this.raftId = raftId;
    }

    getLogInfo() {
        return { raftId: this.raftId };
    }

    getMetricLabels() {
        return {
            logName: 'raft-log',
            logId: this.raftId,
        };
    }
}

module.exports = RaftLogReader;
