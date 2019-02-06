'use strict'; // eslint-disable-line

const testIsOn = process.env.CI === 'true';

const constants = {
    zookeeperNamespace:
        testIsOn ? '/backbeattest/ingestion' : '/backbeat/ingestion',
    zkStatePath: '/state',
    zkStateProperties: ['paused', 'scheduledResume'],
};

module.exports = constants;
