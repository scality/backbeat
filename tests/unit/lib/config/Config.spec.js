'use strict';

const assert = require('assert');

const { Config } = require('../../../../lib/Config');
const backbeatConfig = require('./config.json');

describe('Config', () => {
    let config;
    let testConfig;

    beforeEach(() => {
        config = new Config();
        // deep copy the config to avoid modifying the original
        testConfig = JSON.parse(JSON.stringify(backbeatConfig));
    });

    it('should make the probeserver config in the queuePoulator' +
        'required when multiple extensions are configured', () => {
        delete testConfig.queuePopulator.probeServer;
        assert.throws(() => config._parseConfig(testConfig));
    });

    it('should make the probeserver config in the queuePoulator' +
        'optional when only notification config is specified', () => {
        delete testConfig.queuePopulator.probeServer;
        testConfig.extensions = { notification: testConfig.extensions.notification };
        assert.doesNotThrow(() => config._parseConfig(testConfig));
    });

    it('should throw an error when dataMoverTopic is not provided and transition is supported', () => {
        delete testConfig.extensions.replication.dataMoverTopic;
        testConfig.extensions.lifecycle.supportedLifecycleRules = [
            'Transition',
            'NoncurrentVersionTransition',
            'Expiration',
            'NoncurrentVersionExpiration',
            'AbortIncompleteMultipartUpload',
        ];
        assert.throws(() => config._parseConfig(testConfig));
    });

    it('should make dataMoverTopic optional when transitions are not supported', () => {
        delete testConfig.extensions.replication.dataMoverTopic;
        testConfig.extensions.lifecycle.supportedLifecycleRules = [
            'Expiration',
            'NoncurrentVersionExpiration',
            'AbortIncompleteMultipartUpload',
        ];
        assert.doesNotThrow(() => config._parseConfig(testConfig));
    });
});
