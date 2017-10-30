'use strict'; // eslint-disable-line

const constants = {
    zookeeperReplicationNamespace: '/backbeat/replication',
    proxyVaultPath: '/_/backbeat/vault',
    proxyIAMPath: '/_/backbeat/iam',
    metricsExtension: 'crr',
    metricsTypeQueued: 'queued',
    metricsTypeProcessed: 'processed',
    redisKeys: {
        ops: 'bb:crr:ops',
        bytes: 'bb:crr:bytes',
        opsDone: 'bb:crr:opsdone',
        bytesDone: 'bb:crr:bytesdone',
    },
};

module.exports = constants;
