const assert = require('assert');
const sinon = require('sinon');
const { http: HttpAgent, https: HttpsAgent } = require('httpagent');

const AWS = require('aws-sdk');
const BackbeatMetadataProxy = require('../../../lib/BackbeatMetadataProxy');
const BackbeatClient = require('../../../lib/clients/BackbeatClient');


const fakeLogger = require('../../utils/fakeLogger');

const ClientsManager = require('../../../lib/clients/ClientsManager');

describe('ClientsManager', () => {
    let cm;

    beforeEach(() => {
        cm = new ClientsManager('tests', fakeLogger);
    });

    describe('initCredentialsManager', () => {
        beforeEach(() => {
            cm.initCredentialsManager();
        });
        afterEach(() => {
            cm.credentialsManager.removeAllListeners('deleteCredentials');
            clearInterval(cm._deleteInactiveCredentialsInterval);
        });
        it('should remove clients if credentials expire', () => {
            cm.s3Clients['testClient'] = true;
            cm.backbeatClients['testClient'] = true;
            // simulate expiration of credentials
            cm.credentialsManager.emit('deleteCredentials', 'testClient');
            // clients should be removed
            assert.strictEqual(cm.s3Clients['testClient'], undefined);
            assert.strictEqual(cm.backbeatClients['testClient'], undefined);
        });
        it('should do nothing if credentials not attached to a client', () => {
            cm.s3Clients['testClient'] = true;
            cm.backbeatClients['testClient'] = true;
            // simulate expiration of credentials
            cm.credentialsManager.emit('deleteCredentials', 'nonAttached');
            // clients should still exist
            assert.strictEqual(cm.s3Clients['testClient'], true);
            assert.strictEqual(cm.backbeatClients['testClient'], true);
        });
    });
    
    describe('_generateSTSConfig', () => {
        it('should resolve credentials', () => {
            const authConfig = {
                type: 'assumeRole',
                roleName: 'someRole',
                sts: {
                    host: 'localhost',
                    port: '8900',
                    externalFile: `${__dirname}/utils/credentials.json`,
                },
            };
            const stsConfig = cm._generateSTSConfig(authConfig, 'http', null);
            assert.strictEqual(stsConfig.endpoint, 'http://localhost:8900');
            assert.deepStrictEqual(stsConfig.credentials, {
                accessKeyId: 'HPZYD8TCLETBDZBPNTNQ',
                secretAccessKey: 'prMKjH3Epf2O7+pLA0iwW9OcZjiFVRpHT=7ir3w6',
            });
        });
        it('should keep sts crendentials in the config', () => {
            const authConfig = {
                type: 'assumeRole',
                roleName: 'someRole',
                sts: {
                    host: 'localhost',
                    port: '8900',
                    accessKey: 'access',
                    secretKey: 'secret',
                },
            };
            const stsConfig = cm._generateSTSConfig(authConfig, 'http', null);
            assert.strictEqual(stsConfig.endpoint, 'http://localhost:8900');
            assert.deepStrictEqual(stsConfig.credentials, {
                accessKeyId: 'access',
                secretAccessKey: 'secret',
            });
        });
        it('should return null if auth type is not assumeRole', () => {
            const authConfig = {
                type: 'service',
                roleName: 'someRole',
            };
            const stsConfig = cm._generateSTSConfig(authConfig, 'http', null);
            assert.strictEqual(stsConfig, null);
        });
        it('should use default transport if not specified', () => {
            const authConfig = {
                type: 'assumeRole',
                roleName: 'someRole',
                sts: {
                    host: 'localhost',
                    port: '8900',
                },
            };
            const stsConfig = cm._generateSTSConfig(authConfig, undefined, null);
            assert.strictEqual(stsConfig.endpoint, 'http://localhost:8900');
        });
    });
    
    describe('_createAgents', () => {
        it('should create http agents if transport is http', () => {
            const agents = cm._createAgents('http');
            assert(agents.s3Agent instanceof HttpAgent.Agent);
            assert(agents.stsAgent instanceof HttpAgent.Agent);
        });
        it('should create https agents if transport is https', () => {
            it('should create http agents if transport is http', () => {
                const agents = cm._createAgents('https');
                assert(agents.s3Agent instanceof HttpsAgent.Agent);
                assert(agents.stsAgent instanceof HttpsAgent.Agent);
            });
        });
        it('should default to http agents if transport is not specified', () => {
            const agents = cm._createAgents();
            assert(agents.s3Agent instanceof HttpAgent.Agent);
            assert(agents.stsAgent instanceof HttpAgent.Agent);
        });
    });
    
    describe('_generateClientConfig', () => {
        it('should generate client config with default transport', () => {
            const config = {
                accountId: 'testAccount',
                authConfig: { type: 'account', account: 'bart' },
                s3Config: { host: 'localhost', port: 9000 },
            };
            const clientConfig = cm._generateClientConfig(config);
            assert.strictEqual(clientConfig.accountId, 'testAccount');
            assert.strictEqual(clientConfig.authConfig.type, 'account');
            assert.strictEqual(clientConfig.s3Config.host, 'localhost');
            assert.strictEqual(clientConfig.s3Config.port, 9000);
            assert.strictEqual(clientConfig.transport, 'http');
        });
        it('should generate client config with specified transport', () => {
            const config = {
                accountId: 'testAccount',
                authConfig: { type: 'account', account: 'bart' },
                s3Config: { host: 'localhost', port: 9000 },
                transport: 'https',
            };
            const clientConfig = cm._generateClientConfig(config);
            assert.strictEqual(clientConfig.accountId, 'testAccount');
            assert.strictEqual(clientConfig.authConfig.type, 'account');
            assert.strictEqual(clientConfig.s3Config.host, 'localhost');
            assert.strictEqual(clientConfig.s3Config.port, 9000);
            assert.strictEqual(clientConfig.transport, 'https');
        });
        it('should generate client config with STS config', () => {
            const config = {
                accountId: 'testAccount',
                authConfig: {
                    type: 'assumeRole',
                    roleName: 'someRole',
                    sts: {
                        host: 'localhost',
                        port: 8900,
                        accessKey: 'access',
                        secretKey: 'secret',
                    },
                },
                s3Config: { host: 'localhost', port: 9000 },
            };
            const clientConfig = cm._generateClientConfig(config);
            assert.strictEqual(clientConfig.stsConfig.endpoint, 'http://localhost:8900');
            assert.deepStrictEqual(clientConfig.stsConfig.credentials, {
                accessKeyId: 'access',
                secretAccessKey: 'secret',
            });
        });
    });
    
    describe('setClientConfig', () => {
        it('should set client config', () => {
            const config = {
                accountId: 'testAccount',
                authConfig: { type: 'account', account: 'bart' },
                s3Config: { host: 'localhost', port: 9000 },
            };
            cm.setClientConfig('testClient', config);
            const clientConfig = cm._configs['testClient'];
            assert.strictEqual(clientConfig.accountId, 'testAccount');
            assert.strictEqual(clientConfig.authConfig.type, 'account');
            assert.strictEqual(clientConfig.s3Config.host, 'localhost');
            assert.strictEqual(clientConfig.s3Config.port, 9000);
            assert.strictEqual(clientConfig.transport, 'http');
            assert.strictEqual(clientConfig.stsConfig, null);
            assert(clientConfig.s3Agent instanceof HttpAgent.Agent);
            assert(clientConfig.stsAgent instanceof HttpAgent.Agent);
        });
    });
    
    describe('getClientConfig', () => {
        it('should get client config', () => {
            const config = {
                accountId: 'testAccount',
                authConfig: { type: 'account', account: 'bart' },
                s3Config: { host: 'localhost', port: 9000 },
            };
            cm.setClientConfig('testClient', config);
            const clientConfig = cm.getClientConfig('testClient');
            assert.strictEqual(clientConfig.accountId, 'testAccount');
            assert.strictEqual(clientConfig.authConfig.type, 'account');
            assert.strictEqual(clientConfig.s3Config.host, 'localhost');
            assert.strictEqual(clientConfig.s3Config.port, 9000);
        });
        it('should return null if client config does not exist', () => {
            const clientConfig = cm.getClientConfig('nonExistentClient');
            assert.strictEqual(clientConfig, null);
        });
    });

    describe('getS3Client', () => {
        it('should create an s3 client', () => {
            const config = {
                accountId: 'testAccount',
                authConfig: { type: 'account', account: 'bart' },
                s3Config: { host: 'localhost', port: 9000 },
            };
            cm.setClientConfig('testClient', config);
            const client = cm.getS3Client('testClient');
            assert(client instanceof AWS.S3);
        });
        it('should return null if client does not exist', () => {
            const client = cm.getS3Client('nonExistentClient');
            assert.strictEqual(client, null);
        });
        it('should return null if credentials are not available', () => {
            const config = {
                accountId: 'testAccount',
                authConfig: { type: 'account', account: 'bart' },
                s3Config: { host: 'localhost', port: 9000 },
            };
            cm.setClientConfig('testClient', config);
            cm.credentialsManager.getCredentials = sinon.stub().returns(null);
            const client = cm.getS3Client('testClient');
            assert.strictEqual(client, null);
        });
        it('should return existing client if it exists', () => {
            const config = {
                accountId: 'testAccount',
                authConfig: { type: 'account', account: 'bart' },
                s3Config: { host: 'localhost', port: 9000 },
            };
            cm.setClientConfig('testClient', config);
            const client1 = cm.getS3Client('testClient');
            const client2 = cm.getS3Client('testClient');
            assert.strictEqual(client1, client2);
        });
    });
    
    describe('getBackbeatMetadataProxy', () => {
        it('should create a backbeat metadata proxy', () => {
            const config = {
                accountId: 'testAccount',
                authConfig: { type: 'account', account: 'bart' },
                s3Config: { host: 'localhost', port: 9000 },
            };
            cm.setClientConfig('testClient', config);
            const client = cm.getBackbeatMetadataProxy('testClient');
            assert(client instanceof BackbeatMetadataProxy);
        });
        it('should return null if client does not exist', () => {
            const client = cm.getBackbeatMetadataProxy('nonExistentClient');
            assert.strictEqual(client, null);
        });
    });
    
    describe('getBackbeatClient', () => {
        it('should create an BackbeatClient', () => {
            const config = {
                accountId: 'testAccount',
                authConfig: { type: 'account', account: 'bart' },
                s3Config: { host: 'localhost', port: 9000 },
            };
            cm.setClientConfig('testClient', config);
            const client = cm.getBackbeatClient('testClient');
            assert(client instanceof BackbeatClient);
        });
        it('should return null if client does not exist', () => {
            const client = cm.getBackbeatClient('nonExistentClient');
            assert.strictEqual(client, null);
        });
        it('should return null if credentials are not available', () => {
            const config = {
                accountId: 'testAccount',
                authConfig: { type: 'account', account: 'bart' },
                s3Config: { host: 'localhost', port: 9000 },
            };
            cm.setClientConfig('testClient', config);
            cm.credentialsManager.getCredentials = sinon.stub().returns(null);
            const client = cm.getBackbeatClient('testClient');
            assert.strictEqual(client, null);
        });
        it('should return existing client if it exists', () => {
            const config = {
                accountId: 'testAccount',
                authConfig: { type: 'account', account: 'bart' },
                s3Config: { host: 'localhost', port: 9000 },
            };
            cm.setClientConfig('testClient', config);
            const client1 = cm.getBackbeatClient('testClient');
            const client2 = cm.getBackbeatClient('testClient');
            assert.strictEqual(client1, client2);
        });
    });
    
    describe('removeClients', () => {
        it('should remove clients', () => {
            cm.s3Clients['testClient'] = true;
            cm.backbeatClients['testClient'] = true;
            cm.removeClients('testClient');
            assert.strictEqual(cm.s3Clients['testClient'], undefined);
            assert.strictEqual(cm.backbeatClients['testClient'], undefined);
        });
        it('should do nothing if client does not exist', () => {
            cm.s3Clients['testClient'] = true;
            cm.backbeatClients['testClient'] = true;
            cm.removeClients('nonExistentClient');
            assert.strictEqual(cm.s3Clients['testClient'], true);
            assert.strictEqual(cm.backbeatClients['testClient'], true);
        });
    });
});
