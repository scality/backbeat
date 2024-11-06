'use strict'; // eslint-disable-line

const assert = require('assert');

const { Config } = require('../../../../lib/Config');
const backbeatConfig = require('./config.json');

describe('Config', () => {
    it('should make the probeserver config in the queuePoulator' +
        'required when multiple extensions are configured', () => {
        const config = new Config();
        const testConfig = { ...backbeatConfig };
        delete testConfig.queuePopulator.probeServer;
        assert.throws(() => config._parseConfig(testConfig));
    });

    it('should make the probeserver config in the queuePoulator' +
        'optional when only notification config is specified', () => {
        const config = new Config();
        const testConfig = { ...backbeatConfig };
        delete testConfig.queuePopulator.probeServer;
        testConfig.extensions = { notification: testConfig.extensions.notification };
        assert.doesNotThrow(() => config._parseConfig(testConfig));
    });
});
