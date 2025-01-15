'use strict';

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

    describe('init', () => {
        it('should set vaultClientCache if authConfig type is none', () => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'none' },
                FakeLogger,
            );

            const storeAWSCredentialsPromise = sinon.stub(vaultClientWrapper, '_storeAWSCredentialsPromise').returns();

            vaultClientWrapper.init();
            assert(storeAWSCredentialsPromise.calledOnce);
            assert(vaultClientWrapper._vaultClientCache);
        });

        it('should set vaultClientCache if authConfig type is assumeRole', () => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'assumeRole' },
                FakeLogger,
            );

            const storeAWSCredentialsPromise = sinon.stub(vaultClientWrapper, '_storeAWSCredentialsPromise').returns();

            vaultClientWrapper.init();
            assert(storeAWSCredentialsPromise.calledOnce);
            assert(vaultClientWrapper._vaultClientCache);
        });

        it('should set vaultClientCache if authConfig type is role', () => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'role' },
                FakeLogger,
            );

            const storeAWSCredentialsPromise = sinon.stub(vaultClientWrapper, '_storeAWSCredentialsPromise').returns();

            vaultClientWrapper.init();
            assert(storeAWSCredentialsPromise.notCalled);
        });
    });

    describe('tempCredentialsReady', () => {
        it('should return true if auth type is not assumeRole', () => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'role' },
                FakeLogger,
            );
            assert.strictEqual(vaultClientWrapper.tempCredentialsReady(), true);
        });

        it('should return the value of _tempCredsPromiseResolved', () => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'assumeRole' },
                FakeLogger,
            );
            vaultClientWrapper._tempCredsPromiseResolved = true;
            assert.strictEqual(vaultClientWrapper.tempCredentialsReady(), true);
        });
    });

    describe('getAccountIds', () => {
        it('should return empty result if authConfig type is not assumeRole', done => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'role' },
                FakeLogger,
            );
            vaultClientWrapper.getAccountIds(['account'], (err, res) => {
                assert.ifError(err);
                assert.deepEqual(res, {});
                done();
            });
        });

        it('should call getAccountIdsWithTempCredentials if auth type is assumeRole', done => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'assumeRole' },
                FakeLogger,
            );
            const getAccountIdsWithTempCredentials =
                sinon.stub(vaultClientWrapper, 'getAccountIdsWithTempCredentials').yields();
            vaultClientWrapper.getAccountIds(['account'], err => {
                assert.ifError(err);
                assert(getAccountIdsWithTempCredentials.calledOnce);
                done();
            });
        });

        it('should use non authenticated API when auth type is none', done => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'none' },
                FakeLogger,
                false,
            );
            vaultClientWrapper.init();
            sinon.stub(vaultClientWrapper._vaultClientCache, 'getClient').returns({
                getAccountIds: (canonicalIds, opts, cb) =>
                    cb(null, { message: { body: { account: 'accountID' } } }),
            });
            vaultClientWrapper.getAccountIds(['account'], (err, accountIds) => {
                assert.ifError(err);
                assert.strictEqual(accountIds.account, 'accountID');
                done();
            });
        });
    });

    describe('getAccountIdsWithTempCredentials', () => {
        it('should return accountIds', done => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'role' },
                FakeLogger,
            );
            vaultClientWrapper._tempCredsPromise = Promise.resolve({});
            vaultClientWrapper._vaultClientCache = {
                getClientWithAWSCreds: sinon.stub().resolves({
                    enableIAMOnAdminRoutes: sinon.stub().resolves({
                        getAccountIds: (canonicalIds, opts, cb) =>
                            cb(null, { message: { body: { account: 'accountID' } } }),
                    }),
                }),
            };
            vaultClientWrapper.getAccountIdsWithTempCredentials(['account'], (err, accountIds) => {
                assert.ifError(err);
                assert.strictEqual(accountIds.account, 'accountID');
                done();
            });
        });

        it('should return error if getAccountIds fails', done => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'role' },
                FakeLogger,
            );
            vaultClientWrapper._tempCredsPromise = Promise.resolve({});
            vaultClientWrapper._vaultClientCache = {
                getClientWithAWSCreds: sinon.stub().resolves({
                    enableIAMOnAdminRoutes: sinon.stub().resolves({
                        getAccountIds: (canonicalIds, opts, cb) =>
                            cb(errors.InternalError, null),
                    }),
                }),
            };
            vaultClientWrapper.getAccountIdsWithTempCredentials(['account'], err => {
                assert.deepStrictEqual(err, errors.InternalError);
                done();
            });
        });

        it('should return an error if enableIAMOnAdminRoutes fails', done => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'role' },
                FakeLogger,
            );
            vaultClientWrapper._tempCredsPromise = Promise.resolve({});
            vaultClientWrapper._vaultClientCache = {
                getClientWithAWSCreds: sinon.stub().resolves({
                    enableIAMOnAdminRoutes: sinon.stub().rejects(errors.InternalError),
                }),
            };
            vaultClientWrapper.getAccountIdsWithTempCredentials(['account'], err => {
                assert.deepStrictEqual(err, errors.InternalError);
                done();
            });
        });

        it('should return an error if getClientWithAWSCreds fails', done => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'role' },
                FakeLogger,
            );
            vaultClientWrapper._tempCredsPromise = Promise.resolve({});
            vaultClientWrapper._vaultClientCache = {
                getClientWithAWSCreds: sinon.stub().rejects(errors.InternalError),
            };
            vaultClientWrapper.getAccountIdsWithTempCredentials(['account'], err => {
                assert.deepStrictEqual(err, errors.InternalError);
                done();
            });
        });

        it('should return an error if _tempCredsPromise fails', done => {
            const vaultClientWrapper = new VaultClientWrapper(
                'id',
                { host: '127.0.0.1', port: 8500 },
                { type: 'role' },
                FakeLogger,
            );
            vaultClientWrapper._tempCredsPromise = Promise.reject(errors.InternalError);
            vaultClientWrapper.getAccountIdsWithTempCredentials(['account'], err => {
                assert.deepStrictEqual(err, errors.InternalError);
                done();
            });
        });
    });
});
