'use strict'; // eslint-disable-line

const assert = require('assert');
const sinon = require('sinon');

const FakeLogger = require('../../utils/fakeLogger');
const VaultClientWrapper = require('../../../extensions/utils/VaultClientWrapper');
const { errors } = require('arsenal');

describe('VaultClientWrapper', () => {
    afterEach(() => {
        sinon.restore();
    });

    describe('constructor', () => {
        it('should use vaultAdmin config if provided', () => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'none' },
                FakeLogger,
            );
            assert.strictEqual(vaultClientWrapper._vaultConf.host, '127.0.0.1');
            assert.strictEqual(vaultClientWrapper._vaultConf.port, 8500);
        });

        it('should fallback to authConfig vault if vaultConf is not provided', () => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                undefined,
                { type: 'none', vault: { host: '127.0.0.1', port: 8500 } },
                FakeLogger,
            );
            assert.strictEqual(vaultClientWrapper._vaultConf.host, '127.0.0.1');
            assert.strictEqual(vaultClientWrapper._vaultConf.port, 8500);
        });
    });
});
