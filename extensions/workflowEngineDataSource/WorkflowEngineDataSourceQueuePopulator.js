const async = require('async');
const WorkflowEngineDefs = require('workflow-engine-defs');
const minimatch = require('minimatch');
const uuid = require('uuid/v4');
const NodeZookeeperClient = require('node-zookeeper-client');

const { usersBucket } = require('arsenal').constants;
const ObjectMD = require('arsenal').models.ObjectMD;
const errors = require('arsenal').errors;

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const ObjectQueueEntry = require('../../lib/models/ObjectQueueEntry');

// recheck for new filter descriptors every
const INTERVAL_TIMER_MS = 30000;

/**
 * This class generate events from the queue populator to the various
 * active workflows that contain a data type source.
 */
class WorkflowEngineDataSourceQueuePopulator extends QueuePopulatorExtension {

    constructor(params) {
        super(params);
        this.wed = new WorkflowEngineDefs();
        // active filters
        this.filterDescriptors = {};
        // hash map for ZK children
        this.hashMap = {};
        // set an interval in case of missed events
        // we intentionally set this in the constructor to
        // avoid tests having being woke up
        // we have special treatment to avoid
        // calling uninitialized code
        this.watchInterval = setInterval(() => {
            this.log.debug('WEDSQP: timer');
            this._updateFilterDescriptorsCheckZK();
        }, INTERVAL_TIMER_MS);
    }

    /**
     * Return the hash string used in various maps and redis
     *
     * @param {string} id - the workflow id
     * @param {string} version - the workflow version
     *
     * @return {string} hashstring - the hashtring
     */
    _getHashString(id, version) {
        return `wf_${id}_${version}`;
    }

    /**
     * Load a specified child content in the filterDescriptors
     * map. Perform some sanity checks. Create the entry or update
     * refcount if already exists
     *
     * @note We assume that data sources for a
     * (workflowId,workflowVersion) pair are identical since versions
     * are immutable.
     *
     * @param {string} name - the ZK child name
     * @param {object} value - the filter descriptor object
     *
     * @return {object} validationResult - {isValid, message}
     */
    _loadFilterDescriptor(name, value) {
        this.log.debug(`WEDSQP: loading ${name}`, value);
        // sanity checks
        if (value.workflowId === undefined ||
            value.workflowVersion === undefined ||
            value.subType === undefined ||
            value.nextNodes === undefined) {
            return {
                isValid: false,
                message: 'missing some fields'
            };
        }
        if (value.subType === this.wed.SUB_TYPE_BASIC) {
            if (value.bucket === undefined) {
                return {
                    isValid: false,
                    message: 'basic: missing bucket'
                };
            }
            if (value.key === undefined) {
                return {
                    isValid: false,
                    message: 'basic: missing key'
                };
            }
        }
        // official name of the entry
        const fdName = this._getHashString(
            value.workflowId, value.workflowVersion);
        let fd = this.filterDescriptors[fdName];
        if (fd === undefined) {
            fd = value;
            this.log.info(
                `WEDSQP: adding fd ${fdName} for ${fd.bucket}:${fd.key}`);
            this.filterDescriptors[fdName] = fd;
            fd.name = fdName;
            fd.refCount = 1;
            this.hashMap[name] = fd;
        } else {
            // do not refcount if already registered
            if (!this.hashMap[name]) {
                this.hashMap[name] = fd;
                fd.refCount++;
                this.log.info(
                    `WEDSQP: fd ${fdName} refcount updated ${fd.refCount}`);
            }
        }
        return {
            isValid: true
        };
    }

    /**
     * Returns the number of filter descriptors currently configured
     * (for tests)
     *
     * @return {number} number - the number of filter descriptors
     */
    _getFilterDescriptorsLength() {
        return Object.keys(this.filterDescriptors).length;
    }

    /**
     * Delete all active filter descriptors and hashMap (for tests)
     *
     * @return {undefined}
     */
    _deleteAllFilterDescriptors() {
        this.filterDescriptors = [];
        this.hashMap = {};
    }

    /**
     * check if zkClient exists and is connected before (for timer)
     *
     * @return {undefined}
     */
    _updateFilterDescriptorsCheckZK() {
        if (!this.zkClient) {
            this.log.debug('WEDSQP: zk not initialized');
            return;
        }
        this._updateFilterDescriptors();
    }

    /**
     * Update the filter descriptor table acc/to new information
     *
     * @param {array} results - array of name/values
     *
     * @return {undefined}
     */
    _processFilterDescriptors(results) {
        this.log.debug('WEDSQP: processing filter descriptors');
        // track missing children
        const _children = {};
        // create entries / update refcounts
        Object.values(results).forEach(kv => {
            const name = kv[0];
            const value = JSON.parse(kv[1]);
            _children[name] = true;
            const { isValid, message } =
                  this._loadFilterDescriptor(name, value);
            if (!isValid) {
                this.log.error(
                    `error loading filter descr: ${message}`, {
                        method:
                        'WEDSQP._updateFilterDescriptors',
                        error: errors.InvalidArgument,
                    });
            }
        });
        // unref missing children
        Object.keys(this.hashMap).forEach(key => {
            if (!_children[key]) {
                this.log.debug(
                    `WEDSQP: ${key} not found any more`);
                const fd = this.hashMap[key];
                delete this.hashMap[key];
                fd.refCount--;
                this.log.info(
                    `WEDSQP: fd ${fd.name} refcount updated ${fd.refCount}`);
            }
        });
        // check for unreferenced entries
        Object.keys(this.filterDescriptors).forEach(key => {
            const fd = this.filterDescriptors[key];
            if (fd.refCount === 0) {
                this.log.info(
                    `WEDSQP: deleting filter descr ${key}`);
                delete this.filterDescriptors[key];
            }
        });
    }

    /**
     * Update the filter descriptor table acc/to new information
     *
     * @param {function} [cb] - optional callback when done (for tests)
     *
     * @return {undefined}
     */
    _updateFilterDescriptors(cb) {
        const { zookeeperPath } = this.extConfig;
        this.log.debug(
            `WEDSQP: _updateFilterDescr ${this.filterDescriptors.length}`);
        this.zkClient.getChildren(
            zookeeperPath,
            (err, children) => {
                if (err) {
                    this.log.error(
                        'zookeeper could not get children', {
                            method:
                            'WEDSQP._updateFilterDescriptors',
                            error: err,
                        });
                    if (cb) {
                        return cb(err);
                    }
                    return undefined;
                }
                this.log.debug(
                    `WEDSQP: reading children data ${children.length}`);
                async.map(children, (child, next) => {
                    const kv = [];
                    kv[0] = child;
                    this.zkClient.getData(
                        `${zookeeperPath}/${child}`,
                        (err, data) => {
                            kv[1] = data;
                            next(err, kv);
                        });
                }, (err, results) => {
                    if (err) {
                        this.log.error(
                            'zookeeper get child data error', {
                                method:
                                'WEDSQP._updateFilterDescriptors',
                                error: err,
                            });
                        if (cb) {
                            return cb(err);
                        }
                        return undefined;
                    }
                    this._processFilterDescriptors(results);
                    if (cb) {
                        return cb();
                    }
                    return undefined;
                });
                return undefined;
            });
    }

    /**
     * Monitor the arrival/departure of children in the zookeeperPath
     *
     * @param {Function} cb - callback when watcher set
     *
     * @return {undefined}
     */
    _setZkWatcher(cb) {
        const { zookeeperPath } = this.extConfig;
        this.zkClient.getChildren(
            zookeeperPath,
            event => {
                if (event.type ===
                    NodeZookeeperClient.Event.NODE_CHILDREN_CHANGED) {
                    this.log.debug('WEDSQP: children changed');
                    this._updateFilterDescriptors();
                }
            }, err => {
                if (err) {
                    return cb(err);
                }
                return cb();
            });
    }

    /**
     * Pre-create the zookeeper path for workflow engine data sources,
     * if necessary.
     *
     * @param {Function} cb - The callback function.
     * @param {Boolean} doNotSetWatcher - For tests (filterDescr is
     * set manually)
     * @return {undefined}
     */
    createZkPath(cb, doNotSetWatcher) {
        const { zookeeperPath } = this.extConfig;
        return this.zkClient.getData(zookeeperPath, err => {
            if (err) {
                if (err.name !== 'NO_NODE') {
                    this.log.error('could not get zookeeper node path', {
                        method: 'WEDSQP.createZkPath',
                        error: err,
                    });
                    return cb(err);
                }
                return this.zkClient.mkdirp(zookeeperPath, err => {
                    if (err) {
                        this.log.error('could not create path in zookeeper', {
                            method: 'WEDSQP.createZkPath',
                            zookeeperPath,
                            error: err,
                        });
                        return cb(err);
                    }
                    if (doNotSetWatcher) {
                        return cb();
                    } else {
                        return this._setZkWatcher(cb);
                    }
                });
            }
            if (doNotSetWatcher) {
                return cb();
            } else {
                return this._setZkWatcher(cb);
            }
        });
    }

    /*
     * get log helper
     *
     * @param {object} fd - filter descriptor object
     *
     * @return {string} helper - for logs
     */
    _getLogHelper(fd) {
        return `WEDSQP: ${fd.workflowId}.${fd.workflowVersion}`;
    }

    /**
     * Populates DATA events into the workflow engine
     *
     * @param {Object} entry - The entry as provided by the queue
     * populator
     *
     * @return {undefined}
     */
    filter(entry) {
        // we require an object key name in our entry
        if (entry.key === undefined) {
            return;
        }
        // ignore the internally used `usersBucket` entries. These are used
        // internally in Zenko for specific bucket use-cases
        if (entry.bucket === usersBucket) {
            return;
        }

        const value = JSON.parse(entry.value);
        const queueEntry = new ObjectQueueEntry(entry.bucket, entry.key, value);
        const sanityCheckRes = queueEntry.checkSanity();
        if (sanityCheckRes) {
            this.log.debug('WEDSQP: sanity check failed', {
                entry: queueEntry.getLogInfo(),
            });
            return;
        }

        const bucket = queueEntry.getBucket();
        const key = queueEntry.getObjectKey();
        const objectMD = new ObjectMD(queueEntry.getValue());
        const tags = objectMD.getTags();

        if (this.wed._GUARD in tags) {
            this.log.debug('WEDSQP: skipping guarded entry', {
                entry: queueEntry.getLogInfo(),
            });
            return;
        }

        // iterate over current filter descriptors
        const keys = Object.keys(this.filterDescriptors);
        for (let i = 0; i < keys.length; i++) {
            const fd = this.filterDescriptors[keys[i]];
            const _gLH = this._getLogHelper(fd);
            if (entry.type !== 'put') {
                this.log.debug(`${_gLH}: skipping non matching entry type`, {
                    entry: queueEntry.getLogInfo(),
                });
                // eslint-disable-next-line
                continue;
            }
            let output = false;
            if (fd.subType === this.wed.SUB_TYPE_BASIC) {
                if (minimatch(bucket, fd.bucket)) {
                    if (fd.key) {
                        if (minimatch(key, fd.key)) {
                            output = true;
                        }
                    } else {
                        output = true;
                    }
                }
            } else {
                this.log.error(`${_gLH}: scripts not yet supported`, {
                    entry: queueEntry.getLogInfo(),
                });
                // eslint-disable-next-line
                continue;
            }
            this.log.debug(`${_gLH}: data output`, {
                output
            });
            if (output) {
                // create a topic entry for every next node
                const nextNodes = fd.nextNodes;
                // generate a common uniqueId for all next nodes
                // (will be used for synchronization)
                const uniqueId = uuid();
                nextNodes.forEach(_node => {
                    this.log.debug(
                        `${_gLH}: publishing for ${_node}`, {
                            entry: queueEntry.getLogInfo(),
                        });
                    // set the nodeId for the next step to find itself
                    // and a unique ID to uniquely reference this event
                    const targetEntry = Object.assign({}, entry);
                    targetEntry.workflowId = fd.workflowId;
                    targetEntry.workflowVersion = fd.workflowVersion;
                    targetEntry.nodeId = _node;
                    targetEntry.uniqueId = uniqueId;
                    this.publish(
                        this.extConfig.topic,
                        `${bucket}/${key}`,
                        JSON.stringify(targetEntry));
                });
            }
        }
    }
}

module.exports = WorkflowEngineDataSourceQueuePopulator;
