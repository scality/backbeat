const sinon = require('sinon');
const assert = require('assert');
const zookeeper = require('node-zookeeper-client');
const { Logger } = require('werelogs');
const ZookeeperManager = require('../../../lib/clients/ZookeeperManager');

describe('ZookeeperManager', () => {
    let zkClient;
    let mockClient;
    const log = new Logger('ZookeeperManager:unit');

    beforeEach(() => {
        // Create a mock client
        mockClient = {
            connectionManager: {
                socket: {
                    address: () => ({ address: '127.0.0.1', port: 42000 }),
                },
                xid: 1,
                zxid: Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11, 0xFF]),
                nextServerIndex: 1,
                serverAttempts: 1,
            },
            getSessionId: () => Buffer.from([0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88]),
            getState: () => ({ name: 'SYNC_CONNECTED', code: 3 }),
            on: sinon.stub(),
            once: sinon.stub(),
            connect: sinon.stub(),
            getData: sinon.stub(),
            mkdirp: sinon.stub(),
            create: sinon.stub(),
            close: sinon.stub(),
        };

        // Stub the 'once' method for 'connected' event
        mockClient.once.withArgs('connected').callsFake((event, callback) => callback());

        // Replace the createClient method to return the mock client
        sinon.stub(zookeeper, 'createClient').returns(mockClient);
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should skip if no namespace', done => {
        mockClient.getData.callsArgWith(1, null);
        zkClient = new ZookeeperManager('localhost:2181', { autoCreateNamespace: true }, log);

        zkClient.on('ready', () => {
            sinon.assert.notCalled(mockClient.getData);
            sinon.assert.notCalled(mockClient.mkdirp);
            done();
        });
    });

    it('should not create namespace if it already exists', done => {
        mockClient.getData.callsArgWith(1, null);
        zkClient = new ZookeeperManager('localhost:2181/mynamespace', { autoCreateNamespace: true }, log);

        zkClient.on('ready', () => {
            sinon.assert.calledOnce(mockClient.getData);
            sinon.assert.notCalled(mockClient.mkdirp);
            done();
        });
    });

    it('should create namespace if it does not exist', done => {
        mockClient.getData.callsArgWith(1, { name: 'NO_NODE' });
        mockClient.mkdirp.callsArgWith(1, null);
        zkClient = new ZookeeperManager('localhost:2181/mynamespace', { autoCreateNamespace: true }, log);

        zkClient.on('ready', () => {
            sinon.assert.calledOnce(mockClient.getData);
            sinon.assert.calledOnce(mockClient.mkdirp);
            sinon.assert.calledOnce(mockClient.close);
            done();
        });
    });

    it('should handle getData error', done => {
        mockClient.getData.callsArgWith(1, { name: 'Simulated GET data error' });
        zkClient = new ZookeeperManager('localhost:2181/mynamespace', { autoCreateNamespace: true }, log);

        zkClient.on('error', err => {
            assert(err, 'Error event should be emitted');
            assert.strictEqual(err.name, 'Simulated GET data error');
            done();
        });
    });

    it('should handle mkdirp error', done => {
        mockClient.getData.callsArgWith(1, { name: 'NO_NODE' });
        mockClient.mkdirp.callsArgWith(1, { name: 'Simulated MKDIRP error' });
        zkClient = new ZookeeperManager('localhost:2181/mynamespace', { autoCreateNamespace: true }, log);

        zkClient.on('error', err => {
            assert(err, 'Error event should be emitted');
            assert.strictEqual(err.name, 'Simulated MKDIRP error');
            done();
        });
    });

    it('should create parent path without data and child with data using mkdirpWithChildDataOnly', done => {
        mockClient.mkdirp.callsArgWith(4, null);
        mockClient.create.callsArgWith(4, null);

        zkClient = new ZookeeperManager('localhost:2181', {}, log);

        const testPath = '/config/bucket1';
        const testData = Buffer.from('test-data');

        zkClient.mkdirpWithChildDataOnly(testPath, testData, err => {
            assert.ifError(err);
            sinon.assert.calledOnce(mockClient.mkdirp);
            sinon.assert.calledWith(mockClient.mkdirp, '/config', null);
            sinon.assert.calledOnce(mockClient.create);
            sinon.assert.calledWith(mockClient.create, testPath, testData);
            done();
        });
    });

    it('should handle NODE_EXISTS error on parent path in mkdirpWithChildDataOnly', done => {
        mockClient.mkdirp.callsArgWith(4, { name: 'NODE_EXISTS' });
        mockClient.create.callsArgWith(4, null);

        zkClient = new ZookeeperManager('localhost:2181', {}, log);

        const testPath = '/config/bucket1';
        const testData = Buffer.from('test-data');

        zkClient.mkdirpWithChildDataOnly(testPath, testData, err => {
            assert.ifError(err);
            sinon.assert.calledOnce(mockClient.mkdirp);
            sinon.assert.calledWith(mockClient.mkdirp, '/config', null);
            sinon.assert.calledOnce(mockClient.create);
            sinon.assert.calledWith(mockClient.create, testPath, testData);
            done();
        });
    });
});
