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

    describe('BACKBEAT_CONFIG_OVERRIDES (BB-809)', () => {
        const OVERRIDES_ENV = 'BACKBEAT_CONFIG_OVERRIDES';
        let previous;

        beforeEach(() => {
            previous = process.env[OVERRIDES_ENV];
        });

        afterEach(() => {
            if (previous === undefined) {
                delete process.env[OVERRIDES_ENV];
            } else {
                process.env[OVERRIDES_ENV] = previous;
            }
        });

        it('applies overrides via JSON merge patch and takes precedence over file values', () => {
            const overrideHost = 'override.zk.example:2181';
            assert.notStrictEqual(testConfig.zookeeper.connectionString, overrideHost);

            process.env[OVERRIDES_ENV] = JSON.stringify({
                zookeeper: { connectionString: overrideHost },
                kafka: { maxRequestSize: 12345 },
            });

            config._parseConfig(testConfig);
            assert.strictEqual(config.zookeeper.connectionString, overrideHost);
            assert.strictEqual(config.kafka.maxRequestSize, 12345);
        });

        it('merges nested objects recursively rather than replacing whole subtrees', () => {
            const originalHosts = testConfig.kafka.hosts;
            process.env[OVERRIDES_ENV] = JSON.stringify({
                kafka: { maxRequestSize: 99999 },
            });
            config._parseConfig(testConfig);
            // kafka.hosts must be preserved from the file config
            assert.strictEqual(config.kafka.hosts, originalHosts);
            assert.strictEqual(config.kafka.maxRequestSize, 99999);
        });

        it('validates the merged result and rejects wrong types', () => {
            process.env[OVERRIDES_ENV] = JSON.stringify({
                kafka: { maxRequestSize: 'not-a-number' },
            });
            assert.throws(() => config._parseConfig(testConfig));
        });

        it('validates the merged result and rejects unknown keys', () => {
            process.env[OVERRIDES_ENV] = JSON.stringify({
                thisKeyDoesNotExist: true,
            });
            assert.throws(() => config._parseConfig(testConfig));
        });

        it('throws on invalid JSON in the env var', () => {
            process.env[OVERRIDES_ENV] = '{not-json';
            assert.throws(
                () => config._parseConfig(testConfig),
                /could not parse BACKBEAT_CONFIG_OVERRIDES as JSON/,
            );
        });
    });
});
