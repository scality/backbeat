'use strict'; // eslint-disable-line

const testIsOn = process.env.CI === 'true';

const constants = {
    zookeeperReplicationNamespace: '/backbeat/replication',
    proxyVaultPath: '/_/backbeat/vault',
    proxyIAMPath: '/_/backbeat/iam',
    metricsExtension: 'crr',
    metricsTypeQueued: 'queued',
    metricsTypeCompleted: 'completed',
    metricsTypeFailed: 'failed',
    redisKeys: {
        opsPending: testIsOn ? 'test:bb:opspending' : 'bb:crr:opspending',
        bytesPending: testIsOn ? 'test:bb:bytespending' : 'bb:crr:bytespending',
        ops: testIsOn ? 'test:bb:ops' : 'bb:crr:ops',
        bytes: testIsOn ? 'test:bb:bytes' : 'bb:crr:bytes',
        objectBytes: testIsOn ? 'test:bb:object:bytes' : 'bb:crr:object:bytes',
        opsDone: testIsOn ? 'test:bb:opsdone' : 'bb:crr:opsdone',
        opsFail: testIsOn ? 'test:bb:opsfail' : 'bb:crr:opsfail',
        bytesDone: testIsOn ? 'test:bb:bytesdone' : 'bb:crr:bytesdone',
        objectBytesDone: testIsOn ?
            'test:bb:object:bytesdone' : 'bb:crr:object:bytesdone',
        bytesFail: testIsOn ? 'test:bb:bytesfail' : 'bb:crr:bytesfail',
        failedCRR: testIsOn ? 'test:bb:crr:failed' : 'bb:crr:failed',
    },
};

module.exports = constants;
