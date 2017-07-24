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

class ProvisionDispatcher {

    /**
     * Create a new provision dispatcher object
     *
     * @constructor
     * @param {object} zkConfig - zookeeper config object

     * @param {string} zkConfig.endpoint - zookeeper endpoint string
     * where provisions are managed
     * (e.g. "localhost:2181/shared-tasks")
     * @param {object} logConfig - logging configuration
     * @param {string} logConfig.logLevel - logging level
     * @param {string} logConfig.dumpLevel - dump level
     */
    constructor(zkConfig, logConfig) {
        this.zkEndpoint = zkConfig.endpoint;
        this.client = zookeeper.createClient(zkConfig.endpoint);
        this.client.connect();
        this.connectWaitList = [];
        this.client.once('connected', () => {
            this.log.debug('connected to the ZK server');
            this.connectWaitList.forEach(cb => cb());
            this.connectWaitList = null;
        });
        this.myName = null;
        this.myLeaderName = null;
        this.isLeader = false;
        this.owners = null;
        this.provisions = null;

        this.logger = new Logger('Backbeat:ProvisionDispatcher',
                                 { level: logConfig.logLevel,
                                   dump: logConfig.dumpLevel });
        this.log = this.logger.newRequestLogger();
    }

    /**
     * Add new provisions to the list of provisions to be dispatched
     *
     * @param {array} provisionList - list of provision items as strings
     * @param {function} cb - callback when done
     * @return {undefined}
     */
    addProvisions(provisionList, cb) {
        this.log.debug('add new provisions', { provisionList });
        async.series([
            done => this._waitConnected(done),
            done => this._populateZkHierarchy(done),
            done => async.eachLimit(
                provisionList.map(
                    item => `${ProvisionDispatcher.constants.PROVISIONS}/` +
                        `${item}`),
                20,
                (zkPath, cb) => this.client.create(zkPath, err => {
                    if (err &&
                        err.getCode() !== zookeeper.Exception.NODE_EXISTS) {
                        this.log.error('error adding provision node',
                                       { zkPath: this.zkEndpoint + zkPath,
                                         error: err });
                        return cb(err);
                    }
                    this.log.debug('added new provision zk node',
                                   { zkPath: this.zkEndpoint + zkPath });
                    return cb();
                }),
                done),
        ], cb);
    }

    subscribe(cb) {
        this._waitConnected(() => {
            this._registerLeader(err => {
                if (err) {
                    return cb(err);
                }
                return this._onLeaderChange();
            });
            this._registerOwner(err => {
                if (err) {
                    return cb(err);
                }
                this._onOwnerChange();
                this._onMyselfChange(cb);
                return undefined;
            });
            return this._onProvisionChange();
        });
    }

    unsubscribe(cb) {
        async.parallel([done => this._unregisterOwner(done),
                        done => this._unregisterLeader(done)],
                       cb);
    }

    _waitConnected(cb) {
        if (this.connectWaitList === null) {
            return process.nextTick(cb);
        }
        return this.connectWaitList.push(cb);
    }

    _populateZkHierarchy(done) {
        async.each([
            ProvisionDispatcher.constants.LEADERS,
            ProvisionDispatcher.constants.OWNERS,
            ProvisionDispatcher.constants.PROVISIONS,
        ], (zkPath, cb) => this.client.create(zkPath, err => {
            if (err && err.getCode() !== zookeeper.Exception.NODE_EXISTS) {
                this.log.error('error populating zk node',
                               { zkPath: this.zkEndpoint + zkPath,
                                 error: err });
                return cb(err);
            }
            this.log.debug('populated zk node', { zkPath });
            return cb();
        }), done);
    }

    _redispatchProvisions() {
        this.log.debug('provisions redispatch');
        let ownerIdx = 0;
        const provisionsByOwner = {};
        if (this.owners === null || this.provisions === null ||
            this.owners.length === 0) {
            return undefined;
        }
        // dispatch provisions to owners
        this.owners.forEach(owner => {
            provisionsByOwner[owner] = [];
        });
        this.provisions.forEach(provision => {
            const owner = this.owners[ownerIdx];
            provisionsByOwner[owner].push(provision);
            ownerIdx++;
            if (ownerIdx === this.owners.length) {
                ownerIdx = 0;
            }
        });
        // now write data in owners
        return Object.keys(provisionsByOwner).forEach(owner => {
            const zkPath = `${ProvisionDispatcher.constants.OWNERS}/${owner}`;
            this.log.debug('set new provisions to owner',
                           { zkPath: this.zkEndpoint + zkPath,
                             provisionList: provisionsByOwner[owner] });
            this.client.setData(
                zkPath,
                new Buffer(JSON.stringify(provisionsByOwner[owner])),
                -1, // version
                err => {
                    if (err) {
                        this.log.error('error in setData',
                                       { zkPath: this.zkEndpoint + zkPath,
                                         error: err });
                    }
                });
        });
    }

    _registerLeader(cb) {
        // register in election queue
        const zkPath = (ProvisionDispatcher.constants.LEADERS +
                        ProvisionDispatcher.constants.LEADER);
        this.client.create(
            zkPath, null,
            zookeeper.ACL.OPEN_ACL_UNSAFE,
            zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL,
            (err, _path) => {
                if (err) {
                    return cb(err);
                }
                this.myLeaderName = path.basename(_path);
                this.log.debug('registered leader zk node',
                               { zkPath: this.zkEndpoint + _path });
                return cb();
            });
    }

    _unregisterLeader(cb) {
        // de-register from election queue
        const zkPath = `${ProvisionDispatcher.constants.LEADERS}/` +
                  `${this.myLeaderName}`;
        this.client.remove(zkPath, -1, err => {
            if (err) {
                return cb(err);
            }
            this.log.debug('unregistered leader zk node',
                           { zkPath: this.zkEndpoint + zkPath });
            return cb();
        });
    }

    _registerOwner(cb) {
        this.myName = _genRandomHexChars(12);
        const zkPath = `${ProvisionDispatcher.constants.OWNERS}/` +
                  `${this.myName}`;
        this.client.create(
            zkPath, null, zookeeper.ACL.OPEN_ACL_UNSAFE,
            zookeeper.CreateMode.EPHEMERAL,
            err => {
                if (err) {
                    return cb(err);
                }
                this.log.debug('registered owner zk node',
                               { zkPath: this.zkEndpoint + zkPath });
                return cb(null, zkPath);
            });
    }

    _unregisterOwner(cb) {
        const zkPath = `${ProvisionDispatcher.constants.OWNERS}/` +
                  `${this.myName}`;
        this.client.remove(zkPath, -1, err => {
            if (err) {
                return cb(err);
            }
            this.log.debug('unregistered owner zk node',
                           { zkPath: this.zkEndpoint + zkPath });
            return cb();
        });
    }

    _amILeader(children) {
        return children.every(child => child >= this.myLeaderName);
    }

    _onLeaderChange() {
        // monitor leader change
        const zkPath = ProvisionDispatcher.constants.LEADERS;
        this.client.getChildren(zkPath, event => {
            this.log.debug('got leader event',
                           { zkPath: this.zkEndpoint + zkPath,
                             event });
            this._onLeaderChange();
        }, (err, children) => {
            if (err) {
                this.log.error('list failed',
                               { zkPath: this.zkEndpoint + zkPath,
                                 error: err });
                return;
            }
            this.isLeader = this._amILeader(children);
            this.log.debug('updated leader',
                           { zkPath: this.zkEndpoint + zkPath,
                             children,
                             iamLeader: this.isLeader });
            if (this.isLeader) {
                this._redispatchProvisions();
            }
        });
    }

    _onOwnerChange() {
        const zkPath = ProvisionDispatcher.constants.OWNERS;
        this.client.getChildren(zkPath, event => {
            this.log.debug('got owner event',
                           { zkPath: this.zkEndpoint + zkPath,
                             event });
            this._onOwnerChange();
        }, (err, children) => {
            if (err) {
                this.log.error('list failed',
                               { zkPath: this.zkEndpoint + zkPath,
                                 error: err });
                return;
            }
            this.owners = children;
            this.log.debug('owners updated',
                           { zkPath: this.zkEndpoint + zkPath,
                             owners: this.owners });
            if (this.isLeader) {
                this._redispatchProvisions();
            }
        });
    }

    _onMyselfChange(cb) {
        // monitor change in my content
        const myPath = `${ProvisionDispatcher.constants.OWNERS}/` +
                  `${this.myName}`;
        this.client.getData(
            myPath, event => {
                this.log.debug('got owner self event',
                               { zkPath: this.zkEndpoint + myPath,
                                 event });
                this._onMyselfChange(cb);
            },
            (err, data) => {
                if (err &&
                    err.getCode() !== zookeeper.Exception.NO_NODE) {
                    this.log.error('error in getData',
                                   { zkPath: this.zkEndpoint + myPath,
                                     error: err });
                    return undefined;
                }
                if (data !== undefined) {
                    const provisionList = JSON.parse(data);
                    this.log.info('provisioning update',
                                  { zkPath: this.zkEndpoint + myPath,
                                    provisionList });
                    return cb(null, provisionList);
                }
                return undefined;
            }
        );
    }

    _onProvisionChange() {
        const zkPath = ProvisionDispatcher.constants.PROVISIONS;
        this.client.getChildren(zkPath, event => {
            this.log.debug('got provision event', {
                zkPath: this.zkEndpoint + zkPath,
                event,
            });
            this._onProvisionChange();
        }, (err, children) => {
            if (err) {
                this.log.error('list failed',
                               { zkPath: this.zkEndpoint + zkPath,
                                 error: err });
                return;
            }
            this.provisions = children;
            if (this.isLeader) {
                this._redispatchProvisions();
            }
        });
    }
}

ProvisionDispatcher.constants = {
    LEADERS: '/leaders',
    LEADER: '/leader',
    OWNERS: '/owners',
    PROVISIONS: '/provisions',
};

module.exports = ProvisionDispatcher;
