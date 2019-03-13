'use strict'; // eslint-disable-line

const testIsOn = process.env.CI === 'true';

const constants = {
    zookeeperNamespace:
        testIsOn ? '/backbeattest/ingestion' : '/backbeat/ingestion',
    zkStatePath: '/state',
    zkStateProperties: ['paused', 'scheduledResume'],
    metricsExtension: 'ingestion',
    redisKeys: {
        opsDone: testIsOn ? 'test:bb:opsdone' : 'bb:ingestion:opsdone',
        bytesDone: testIsOn ? 'test:bb:bytesdone' : 'bb:ingestion:bytesdone',
        opsPending: testIsOn ? 'test:bb:opspending' : 'bb:ingestion:opspending',
        bytesPending: testIsOn ?
            'test:bb:bytespending' : 'bb:ingestion:bytespending',
    },
};

module.exports = constants;
