const arsenal = require('arsenal');
const LogConsumer = arsenal.storage.metadata.LogConsumer;
const BucketClient = require('bucketclient').RESTClient;

const LogReader = require('./LogReader');

class RaftLogReader extends LogReader {
    constructor(params) {
        const { zkClient, zkConfig, bucketdConfig, httpsConfig,
                raftId, logger, extensions, metricsProducer } = params;
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
        super({ zkClient, zkConfig, logConsumer, logId: `raft_${raftId}`,
                logger, extensions, metricsProducer });
        this.raftId = raftId;
    }

    getLogInfo() {
        return { raftId: this.raftId };
    }
}

module.exports = RaftLogReader;
