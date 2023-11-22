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
            on: sinon.stub(),
            once: sinon.stub(),
            connect: sinon.stub(),
            getData: sinon.stub(),
            mkdirp: sinon.stub(),
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
});
