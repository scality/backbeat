const arsenal = require('arsenal');
const LogConsumer = arsenal.storage.metadata.LogConsumer;
const BucketClient = require('bucketclient').RESTClient;

const LogReader = require('./LogReader');

class RaftLogReader extends LogReader {
    constructor(params) {
        const { zkClient, kafkaProducer, bucketdConfig, raftId, logger }
                  = params;
        const { host, port } = bucketdConfig;
        logger.info('initializing raft log reader',
            { method: 'RaftLogReader.constructor',
                bucketdConfig, raftId });
        const bucketClient = new BucketClient(`${host}:${port}`);
        const logConsumer = new LogConsumer({ bucketClient,
            raftSession: raftId,
            logger });
        super({ zkClient, kafkaProducer, logConsumer,
            logId: `raft_${raftId}`, logger });
        this.raftId = raftId;
    }

    getLogInfo() {
        return { raftId: this.raftId };
    }
}

module.exports = RaftLogReader;
