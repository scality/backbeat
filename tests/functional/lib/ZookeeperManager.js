const assert = require('assert');
const werelogs = require('werelogs');
const async = require('async');

const ZookeeperManager = require('../../../lib/clients/ZookeeperManager');
const zookeeper = require('node-zookeeper-client');

const log = new werelogs.Logger('ZookeeperManager:test');

// simulateSessionExpired is designed to artificially trigger a session expiration in a ZooKeeper client
function simulateSessionExpired(client) {
    client.connectionManager.setState(-3); // SESSION_EXPIRED EVENT CODE
    client.connectionManager.socket.emit('close');
}

describe('ZookeeperManager', () => {
    let zkClient;

    afterEach(() => {
        // Clean up and reset the ZookeeperManager
        zkClient.removeAllListeners();
        zkClient.close();
    });

    describe('with autoCreateNamespace', () => {
        const basePath = '/hello/world';
        let rootZkClient;

        before(done => {
            rootZkClient = zookeeper.createClient('localhost:2181');
            rootZkClient.connect();
            rootZkClient.once('connected', () => done());
        });

        after(() => rootZkClient.close());

        afterEach(done => {
            rootZkClient.remove(basePath, err => {
                if (err && err.name !== 'NO_NODE') {
                    return done(err);
                }
                return rootZkClient.remove('/hello', err => {
                    if (err && err.name !== 'NO_NODE') {
                        return done(err);
                    }
                    return done();
                });
            });
        });

        it('should create the base path if autoCreateNamespace set to true', done => {
            const connectionString = `localhost:2181${basePath}`;
            const options = { autoCreateNamespace: true };
            zkClient = new ZookeeperManager(connectionString, options, log);
            zkClient.once('ready', () => {
                // makes sure the base path exists
                rootZkClient.getData(basePath, err => {
                    assert.ifError(err);

                    // makes sure no NO_NODE error is returned.
                    zkClient.getData('/', err => {
                        assert.ifError(err);
                        done();
                    });
                });
            });
        });

        it('should skip and fire ready event if autoCreateNamespace is true with no namespace', done => {
            const connectionStringWithoutNamespace = 'localhost:2181';
            const options = { autoCreateNamespace: true };
            zkClient = new ZookeeperManager(connectionStringWithoutNamespace, options, log);
            zkClient.once('ready', () => zkClient.getData('/', err => {
                assert.ifError(err);
                done();
            }));
        });

        it('should skip and fire ready event if autoCreateNamespace is true with / namespace', done => {
            const connectionStringWithoutNamespace = 'localhost:2181/';
            const options = { autoCreateNamespace: true };
            zkClient = new ZookeeperManager(connectionStringWithoutNamespace, options, log);
            zkClient.once('ready', () => zkClient.getData('/', err => {
                assert.ifError(err);
                done();
            }));
        });

        it('should not create the base path if autoCreateNamespace set to false', done => {
            const connectionString = `localhost:2181${basePath}`;
            const options = { autoCreateNamespace: false };
            zkClient = new ZookeeperManager(connectionString, options, log);
            zkClient.once('ready', () => {
                rootZkClient.getData(basePath, err => {
                    assert(err, 'Expected an error to be returned');
                    assert.strictEqual(err.name, 'NO_NODE');

                    zkClient.getData('/', err => {
                        assert(err, 'Expected an error to be returned');
                        assert.strictEqual(err.name, 'NO_NODE');
                        done();
                    });
                });
            });
        });
    });

    describe('testing connection events', () => {
        beforeEach(done => {
            const connectionString = 'localhost:2181';
            const options = {};
            zkClient = new ZookeeperManager(connectionString, options, log);
            zkClient.once('connected', () => done());
        });

        it('should trigger the disconnected event after closing the connection', done => {
            zkClient.on('disconnected', () => done());
            zkClient.close();
        });

        it('should handle "expired" event and reconnect', done => {
            const originalClient = zkClient.client;
            simulateSessionExpired(originalClient);
            assert.notStrictEqual(zkClient.client, originalClient);
            // the original connection should be disconnected
            originalClient.getData('/', err => {
                assert(err, 'Expected an error to be returned');
                assert.strictEqual(err.name, 'CONNECTION_LOSS');

                // the client should have reconnected and work
                zkClient.getData('/', err => {
                    assert.ifError(err);
                    done();
                });
            });
        });

        it('should close the client connection', done => {
            zkClient.close();
            zkClient.getData('/', err => {
                assert(err, 'Expected an error to be returned');
                assert.strictEqual(err.name, 'CONNECTION_LOSS');
                done();
            });
        });
    });

    describe('CRUD', () => {
        const path = '/testNode';
        beforeEach(done => {
            const connectionString = 'localhost:2181';
            const options = {};
            zkClient = new ZookeeperManager(connectionString, options, log);
            zkClient.on('connected', () => done());
        });

        afterEach(done => {
            zkClient.removeRecur(path, err => {
                if (err && err.name !== 'NO_NODE') {
                    return done(err);
                }
                return done();
            });
        });

        it('should create a node with a given data', done => {
            const expectedValue = 'testData';
            const data = Buffer.from(expectedValue);
            zkClient.create(path, data, err => {
                assert.ifError(err);
                zkClient.getData(path, (err, data) => {
                    assert.ifError(err);
                    assert.strictEqual(data.toString(), expectedValue);
                    done();
                });
            });
        });

        it('should create a node and check that it exists', done => {
            zkClient.create(path, err => {
                assert.ifError(err);
                zkClient.exists(path, (err, stat) => {
                    assert.ifError(err);
                    assert(stat);
                    done();
                });
            });
        });

        it('should check that a node does not exist', done => {
            zkClient.exists(path, (err, stat) => {
                assert.ifError(err);
                assert(!stat);
                done();
            });
        });

        it('should create intermediate node', done => {
            const complexPath = '/testNode/second';
            const expectedValue = 'testData';
            const data = Buffer.from(expectedValue);
            zkClient.mkdirp(complexPath, data, (err, returnedPath) => {
                assert.ifError(err);
                assert.strictEqual(returnedPath, complexPath);
                zkClient.getChildren('/testNode', (err, children) => {
                    assert.ifError(err);
                    assert.strictEqual(children.length, 1);
                    const child = children[0];
                    assert.strictEqual(child, 'second');
                    done();
                });
            });
        });

        it('should create a node and set, get data and then remove the node', done => {
            const expectedValue = 'testData';
            async.series([
                next => zkClient.create(path, err => {
                    assert.ifError(err);
                    next();
                }),
                next => zkClient.setData(path, Buffer.from(expectedValue), err => {
                    assert.ifError(err);
                    next();
                }),
                next => zkClient.getData(path, (err, data) => {
                    assert.ifError(err);
                    assert.strictEqual(data.toString(), expectedValue);
                    next();
                }),
                next => zkClient.remove(path, err => {
                    assert.ifError(err);
                    next();
                }),
                next => zkClient.getData(path, err => {
                    assert(err, 'Expected an error to be returned');
                    assert.strictEqual(err.name, 'NO_NODE');
                    next();
                }),
            ], done);
        });

        it('should return the client state', () => {
            assert.strictEqual(zkClient.getState(), zookeeper.State.SYNC_CONNECTED);
        });
    });

    describe('removeRecur', () => {
        beforeEach(done => {
            const connectionString = 'localhost:2181';
            const options = {};
            zkClient = new ZookeeperManager(connectionString, options, log);
            zkClient.on('connected', () => {
                zkClient.mkdirp('/to/be/recursively/removed', err => done(err));
            });
        });

        it('should recursively remove a node and all of its children', done => {
            zkClient.removeRecur('/to', err => {
                assert.ifError(err);
                zkClient.getData('/to', err => {
                    assert(err, 'Expected an error to be returned');
                    assert.strictEqual(err.name, 'NO_NODE');
                    done();
                });
            });
        });
    });

    describe('setOrCreate', () => {
        const alreadyCreatedNodePath = '/to/set/data/in';
        beforeEach(done => {
            const connectionString = 'localhost:2181';
            const options = {};
            zkClient = new ZookeeperManager(connectionString, options, log);
            zkClient.on('connected', () => zkClient.mkdirp(alreadyCreatedNodePath, err => done(err)));
        });

        afterEach(done => zkClient.removeRecur('/to', err => done(err)));

        it('should create the node since it does not exist', done => {
            const expectedValue = 'val';
            const data = Buffer.from(expectedValue);
            const nodePath = '/to/be/created';
            zkClient.setOrCreate(nodePath, data, err => {
                assert.ifError(err);
                zkClient.getData(nodePath, (err, data) => {
                    assert.ifError(err);
                    assert.strictEqual(data.toString(), expectedValue);
                    done(err);
                });
            });
        });

        it('should set the data for a node that already exists', done => {
            const expectedValue = 'val';
            const data = Buffer.from(expectedValue);
            zkClient.setOrCreate(alreadyCreatedNodePath, data, err => {
                assert.ifError(err);
                zkClient.getData(alreadyCreatedNodePath, (err, data) => {
                    assert.ifError(err);
                    assert.strictEqual(data.toString(), expectedValue);
                    done(err);
                });
            });
        });
    });
});
