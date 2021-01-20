'use strict'; // eslint-disable-line

const async = require('async');
const crypto = require('crypto');
const path = require('path');
const zookeeper = require('node-zookeeper-client');

const Logger = require('werelogs').Logger;

function _genRandomHexChars(len) {
    return crypto.randomBytes(Math.ceil(len / 2))
        .toString('hex') // convert to hexadecimal format
        .slice(0, len);  // return required number of characters
}

const LEADERS = '/leaders';
const LEADER = '/leader';
const OWNERS = '/owners';
const PROVISIONS = '/provisions';
const PERIODIC_CHECK_TIMEOUT = 60000;

class ProvisionDispatcher {

    /**
     * Create a new provision dispatcher object
     *
     * @constructor
     * @param {object} zkConfig - zookeeper config object
     * @param {string} zkConfig.connectionString - zookeeper connection string
     * (e.g. "localhost:2181/shared-tasks")
     * @param {string} [_zookeeper] - zookeeper module
     */
    constructor(zkConfig, _zookeeper) {
        this._zkEndpoint = zkConfig.connectionString;
        this._zookeeper = (_zookeeper !== undefined) ? _zookeeper : zookeeper;
        this._doRandDelay = false;
        this._client = this._zookeeper.createClient(zkConfig.connectionString);
        this._connectWaitList = [];
        this._client.once('connected', () => {
            this._log.debug('connected to the ZK server');
            this._connectWaitList.forEach(cb => cb());
            this._connectWaitList = null;
        });
        this._client.connect();
        this._myName = null;
        this._myLeaderName = null;
        this._isLeader = false;
        this._owners = null;
        this._provisions = null;
        this._redispatchInProgress = false;
        this._redoRedispatch = false;
        this._interval = -1;

        this._log = new Logger('Backbeat:ProvisionDispatcher');
    }

    /**
     * Create artificial random delays (for tests)
     *
     * @param {boolean} [_doRandDelay] - introduce random delays
     * where provisions are managed
     * @return {undefined}
     */
    _setDoRandDelay(_doRandDelay = false) {
        this._doRandDelay = _doRandDelay;
    }

    _withRandomDelayIfTest(cb) {
        if (this._doRandDelay) {
            setTimeout(cb, Math.random() * 100);
        } else {
            cb();
        }
    }

    /**
     * Add new provisions to the list of provisions to be dispatched
     *
     * @param {array} provisionList - list of provision items as strings
     * @param {function} cb - callback when done
     * @return {undefined}
     */
    addProvisions(provisionList, cb) {
        this._log.debug('add new provisions', { provisionList });
        async.series([
            next => this._waitConnected(next),
            next => this._populateZkHierarchy(next),
            next => this._addProvisionZkNodes(provisionList, next),
        ], cb);
    }

    checkState(cb, autoTrigger) {
        this._onLeaderChange(autoTrigger);
        this._onOwnerChange(autoTrigger);
        this._onMyselfChange(cb, autoTrigger);
        this._onProvisionChange(autoTrigger);
        return undefined;
    }

    subscribe(cb,
              doPeriodicCheck = true,
              periodicCheckTimeout = PERIODIC_CHECK_TIMEOUT) {
        this._waitConnected(() => {
            async.parallel([
                done => this._withRandomDelayIfTest(
                    () => this._registerLeader(done)),
                done => this._withRandomDelayIfTest(
                    () => this._registerOwner(done)),
            ], err => {
                if (err) {
                    return cb(err);
                }
                if (doPeriodicCheck) {
                    this._interval = setInterval(() => {
                        this._log.info('periodic check state');
                        this.checkState(cb, false);
                    }, periodicCheckTimeout);
                }
                return this.checkState(cb, true);
            });
        });
    }

    unsubscribe(cb) {
        if (this._interval !== -1) {
            clearInterval(this._interval);
            this._interval = -1;
        }
        async.parallel([
            next => this._unregisterOwner(next),
            next => this._unregisterLeader(next),
        ], cb);
    }

    _waitConnected(cb) {
        if (this._connectWaitList === null) {
            return process.nextTick(cb);
        }
        return this._connectWaitList.push(cb);
    }

    _populateZkHierarchy(cb) {
        async.each([
            LEADERS,
            OWNERS,
            PROVISIONS,
        ], (zkPath, done) => this._client.create(zkPath, err => {
            if (err && err.getCode() !== zookeeper.Exception.NODE_EXISTS) {
                this._log.error('error populating zk node',
                    { zkPath: `${this._zkEndpoint}${zkPath}`,
                        error: err });
                return done(err);
            }
            this._log.debug('populated zk node', { zkPath });
            return done();
        }), cb);
    }

    _addProvisionZkNodes(provisionList, cb) {
        async.eachLimit(
            provisionList.map(
                item => `${PROVISIONS}/${item}`),
            20,
            (zkPath, done) => this._client.create(zkPath, err => {
                if (err &&
                    err.getCode() !== zookeeper.Exception.NODE_EXISTS) {
                    this._log.error('error adding provision node',
                        { zkPath: `${this._zkEndpoint}${zkPath}`,
                            error: err });
                    return done(err);
                }
                this._log.debug('added new provision zk node',
                               { zkPath: `${this._zkEndpoint}${zkPath}` });
                return done();
            }), cb);
    }

    _redispatchProvisions() {
        this._log.debug('provisions redispatch');
        let ownerIdx = 0;
        const provisionsByOwner = {};
        if (this._owners === null || this._provisions === null ||
            this._owners.length === 0) {
            return undefined;
        }
        if (this._redispatchInProgress) {
            // queue up a redispatch after the current one completes
            // to avoid race conditions with owner changes
            this._redoRedispatch = true;
            return undefined;
        }
        this._redispatchInProgress = true;
        // dispatch provisions to owners
        this._owners.forEach(owner => {
            provisionsByOwner[owner] = [];
        });
        this._provisions.forEach(provision => {
            const owner = this._owners[ownerIdx];
            provisionsByOwner[owner].push(provision);
            ownerIdx++;
            if (ownerIdx === this._owners.length) {
                ownerIdx = 0;
            }
        });
        // now write data in owners
        const _provisionOwner = (owner, done) => {
            const zkPath = `${OWNERS}/${owner}`;
            this._log.debug('set new provisions to owner',
                { zkPath: `${this._zkEndpoint}${zkPath}`,
                    provisionList: provisionsByOwner[owner] });
            const dataString = JSON.stringify(provisionsByOwner[owner]);
            const strLength = Buffer.byteLength(dataString);
            const data = Buffer.alloc(strLength, dataString);
            const version = -1;
            this._withRandomDelayIfTest(
                () => this._client.setData(zkPath, data, version, err => {
                    if (err && err.getCode() !== zookeeper.Exception.NO_NODE) {
                        this._log.error('error in setData', {
                            zkPath: `${this._zkEndpoint}${zkPath}`,
                            error: err,
                        });
                        return done(err);
                    }
                    return done();
                })
            );
        };
        return this._withRandomDelayIfTest(
            () => async.eachLimit(
                Object.keys(provisionsByOwner), 20, _provisionOwner, () => {
                    this._redispatchInProgress = false;
                    if (this._redoRedispatch) {
                        this._log.debug('redoing provisions redispatch');
                        this._redoRedispatch = false;
                        this._redispatchProvisions();
                    }
                }));
    }

    _registerLeader(cb) {
        // register in election queue
        const zkPath = `${LEADERS}${LEADER}`;
        this._withRandomDelayIfTest(
            () => this._client.create(
                zkPath, null,
                zookeeper.ACL.OPEN_ACL_UNSAFE,
                zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL,
                (err, _path) => {
                    if (err) {
                        return cb(err);
                    }
                    this._myLeaderName = path.basename(_path);
                    this._log.debug('registered leader zk node',
                                    { zkPath: `${this._zkEndpoint}${path}` });
                    return cb();
                })
        );
    }

    _unregisterLeader(cb) {
        // de-register from election queue
        const zkPath = `${LEADERS}/${this._myLeaderName}`;
        this._withRandomDelayIfTest(
            () => this._client.remove(zkPath, -1, err => {
                if (err) {
                    return cb(err);
                }
                this._log.debug('unregistered leader zk node',
                                { zkPath: `${this._zkEndpoint}${zkPath}` });
                return cb();
            })
        );
    }

    _registerOwner(cb) {
        this._myName = _genRandomHexChars(12);
        const zkPath = `${OWNERS}/${this._myName}`;
        this._withRandomDelayIfTest(
            () => this._client.create(
                zkPath, null, zookeeper.ACL.OPEN_ACL_UNSAFE,
                zookeeper.CreateMode.EPHEMERAL,
                err => {
                    if (err) {
                        return cb(err);
                    }
                    this._log.debug('registered owner zk node',
                                    { zkPath: `${this._zkEndpoint}${zkPath}` });
                    return cb(null, zkPath);
                })
        );
    }

    _unregisterOwner(cb) {
        const zkPath = `${OWNERS}/${this._myName}`;
        this._withRandomDelayIfTest(
            () => this._client.remove(zkPath, -1, err => {
                if (err) {
                    return cb(err);
                }
                this._log.debug('unregistered owner zk node',
                                { zkPath: `${this._zkEndpoint}${zkPath}` });
                return cb();
            })
        );
    }

    _amILeader(children) {
        return children.every(child => child >= this._myLeaderName);
    }

    _myGetChildren(autoTrigger, path, watcher, callback) {
        if (autoTrigger) {
            return this._client.getChildren(path, watcher, callback);
        }
        return this._client.getChildren(path, callback);
    }

    _onLeaderChange(autoTrigger) {
        // monitor leader change
        const zkPath = LEADERS;
        this._withRandomDelayIfTest(
            () => this._myGetChildren(autoTrigger, zkPath, event => {
                this._log.debug('got leader event',
                                { zkPath: `${this._zkEndpoint}${zkPath}`,
                                  event });
                this._onLeaderChange(true);
            }, (err, children) => {
                if (err) {
                    this._log.error('list failed',
                                    { zkPath: `${this._zkEndpoint}${zkPath}`,
                                      error: err });
                    return;
                }
                this._isLeader = this._amILeader(children);
                this._log.debug('updated leader',
                                { zkPath: `${this._zkEndpoint}${zkPath}`,
                                  children,
                                  iamLeader: this._isLeader });
                if (this._isLeader) {
                    this._redispatchProvisions();
                }
            })
        );
    }

    _onOwnerChange(autoTrigger) {
        const zkPath = OWNERS;
        this._withRandomDelayIfTest(
            () => this._myGetChildren(autoTrigger, zkPath, event => {
                this._log.debug('got owner event',
                                { zkPath: `${this._zkEndpoint}${zkPath}`,
                                  event });
                this._onOwnerChange(true);
            }, (err, children) => {
                if (err) {
                    this._log.error('list failed',
                                    { zkPath: `${this._zkEndpoint}${zkPath}`,
                                      error: err });
                    return;
                }
                this._owners = children;
                this._log.debug('owners updated',
                                { zkPath: `${this._zkEndpoint}${zkPath}`,
                                  owners: this._owners });
                if (this._isLeader) {
                    this._redispatchProvisions();
                }
            })
        );
    }

    _myGetData(autoTrigger, path, watcher, callback) {
        if (autoTrigger) {
            return this._client.getData(path, watcher, callback);
        }
        return this._client.getData(path, callback);
    }

    _getMyPath() {
        return `${OWNERS}/${this._myName}`;
    }

    _onMyselfChange(cb, autoTrigger) {
        // monitor change in my content
        const myPath = `${OWNERS}/${this._myName}`;
        this._withRandomDelayIfTest(
            () => this._myGetData(
                autoTrigger, myPath, event => {
                    this._log.debug('got owner self event',
                                    { zkPath: this._zkEndpoint + myPath,
                                      event });
                    this._onMyselfChange(cb, true);
                },
                (err, data) => {
                    if (err &&
                        err.getCode() !== zookeeper.Exception.NO_NODE) {
                        this._log.error('error in getData',
                                        { zkPath: this._zkEndpoint + myPath,
                                          error: err });
                        return cb(err);
                    }
                    if (data !== undefined) {
                        const provisionList = JSON.parse(data);
                        this._log.info('provisioning update',
                                       { zkPath: this._zkEndpoint + myPath,
                                         provisionList });
                        return cb(null, provisionList);
                    }
                    return undefined;
                }
            )
        );
    }

    _onProvisionChange(autoTrigger) {
        const zkPath = PROVISIONS;
        this._withRandomDelayIfTest(
            () => this._myGetChildren(autoTrigger, zkPath, event => {
                this._log.debug('got provision event', {
                    zkPath: `${this._zkEndpoint}${zkPath}`,
                    event,
                });
                this._onProvisionChange(true);
            }, (err, children) => {
                if (err) {
                    this._log.error('list failed',
                                    { zkPath: `${this._zkEndpoint}${zkPath}`,
                                      error: err });
                    return;
                }
                this._provisions = children;
                if (this._isLeader) {
                    this._redispatchProvisions();
                }
            })
        );
    }
}

module.exports = ProvisionDispatcher;
