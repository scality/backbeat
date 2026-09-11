'use strict';

const async = require('async');
const fs = require('fs');
const { errors } = require('arsenal');
const { ZenkoMetrics } = require('arsenal').metrics;

const ZookeeperManager = require('../../../lib/clients/ZookeeperManager');
const safeJsonParse = require('../../../lib/util/safeJsonParse');

// bounded retry with backoff when re-arming the watch, so a transient
// zookeeper failure does not leave the worker blind to later changes
const REARM_MAX_ATTEMPTS = 5;
const REARM_RETRY_BASE_MS = 200;

const workgroupGeneration = ZenkoMetrics.createGauge({
    name: 's3_notification_delivery_worker_workgroup_generation',
    help: 'Generation of the workgroups configuration this worker is running',
    labelNames: ['workgroup', 'source'],
});

const workgroupConfigChanges = ZenkoMetrics.createCounter({
    name: 's3_notification_delivery_worker_workgroup_config_changes_total',
    help: 'Number of workgroups configuration changes observed in zookeeper ' +
        'since this worker started',
    labelNames: ['workgroup'],
});

/**
 * @class WorkgroupConfigLoader
 *
 * @classdesc Reads the single znode holding the workgroups document, checks
 * it against what this worker was deployed to run, and keeps a copy on disk
 * so a worker can still start while zookeeper is unreachable.
 *
 * The document is read once, at startup. A change seen afterwards is logged
 * and counted but does not touch the running worker: a cutover is safe only
 * because the new generation's consumer groups are pre-seeded while they are
 * empty, which a running process cannot arrange for itself.
 */
class WorkgroupConfigLoader {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.zkConfig - the config.zookeeper block
     * @param {Object} params.workgroupsConfig - deliveryPool.workgroups
     * @param {String} params.topic - unprefixed delivery topic name, checked
     *   against the document
     * @param {String} params.workgroupId - resolved workgroup id, checked
     *   against the document
     * @param {Logger} params.logger - werelogs logger
     * @param {Object} [params.zkClient] - zookeeper client, for tests
     * @param {Function} [params.validate] - document validator, defaults to
     *   validateWorkgroupsDoc
     */
    constructor(params) {
        this._zkConfig = params.zkConfig;
        this._workgroupsConfig = params.workgroupsConfig;
        this._zkPath = params.workgroupsConfig.zookeeperPath;
        this._cachePath = params.workgroupsConfig.cachePath;
        this._pinnedGeneration = params.workgroupsConfig.generation;
        this._topic = params.topic;
        this._workgroupId = params.workgroupId;
        this.log = params.logger;
        this._zkClient = params.zkClient || null;
        this._ownsClient = !params.zkClient;
        this._validate = params.validate || null;
        this._doc = null;
        this._watchArmed = false;
        this._boundWatcher = event => this._watcher(event);
    }

    /**
     * Validates a document. The membership module is resolved on first use
     * rather than at module load, so an injected validator is enough to
     * build this class.
     *
     * @param {Object} doc - parsed document
     * @return {Object} { error, value }
     */
    _validateDoc(doc) {
        if (!this._validate) {
            this._validate =
                require('../utils/workgroups').validateWorkgroupsDoc;
        }
        return this._validate(doc);
    }

    /**
     * The document this worker is running, null before load() succeeds
     *
     * @return {Object|null} workgroups document
     */
    getConfig() {
        return this._doc;
    }

    /**
     * Reads the workgroups document, from zookeeper when it can be reached
     * and from the on-disk cache otherwise
     *
     * @param {Function} done - callback: done(err, { doc, source })
     * @return {undefined}
     */
    load(done) {
        return this._setupZookeeper(setupErr => {
            if (setupErr) {
                return this._loadFromCache('could not connect to zookeeper',
                    setupErr, done);
            }
            return this._zkClient.getData(this._zkPath, this._boundWatcher,
                (getErr, data) => {
                    if (getErr) {
                        return this._loadFromCache('could not read the ' +
                            'workgroups document from zookeeper', getErr, done);
                    }
                    this._watchArmed = true;
                    const { error, doc } = this._checkDocument(data);
                    if (error) {
                        // the cutover tool validates the document with the
                        // same function before writing it, so a broken
                        // document can only be a hand edit: masking it with a
                        // stale cache half applies a cutover across a fleet
                        this.log.error('the workgroups document in zookeeper ' +
                            'is unusable', {
                            method: 'WorkgroupConfigLoader.load',
                            zkPath: this._zkPath,
                            error: error.description || error.message,
                        });
                        return done(error);
                    }
                    return this._writeCache(doc, () =>
                        this._accept(doc, 'zookeeper', done));
                });
        });
    }

    /**
     * Reads the per destination watermarks seeded for the generation this
     * worker runs, or null when the seeding tool wrote none
     *
     * @param {Function} done - callback: done(err, watermarks)
     * @return {undefined}
     */
    loadWatermarks(done) {
        if (!this._doc || !this._zkClient) {
            return process.nextTick(() => done(null, null));
        }
        const { buildWatermarksPath, validateWatermarksDoc } =
            require('../utils/workgroups');
        const path = buildWatermarksPath(this._zkPath, this._doc.generation);
        return this._zkClient.getData(path, undefined, (err, data) => {
            if (err) {
                if (err.name === 'NO_NODE') {
                    this.log.info('no watermarks seeded for this generation, ' +
                        'every matching record is delivered', {
                        method: 'WorkgroupConfigLoader.loadWatermarks',
                        path,
                    });
                    return done(null, null);
                }
                return done(err);
            }
            let parsed;
            try {
                parsed = JSON.parse(data);
            } catch (parseErr) {
                return done(errors.InternalError.customizeDescription(
                    `the watermarks document at ${path} is not valid JSON: ` +
                    `${parseErr.message}`));
            }
            const { error, value } = validateWatermarksDoc(parsed);
            if (error) {
                return done(errors.InternalError.customizeDescription(
                    `the watermarks document at ${path} is invalid: ` +
                    `${error.message}`));
            }
            this.log.info('loaded the per destination watermarks', {
                method: 'WorkgroupConfigLoader.loadWatermarks',
                path,
                destinations: Object.keys(value).length,
            });
            return done(null, value);
        });
    }

    /**
     * Arms the one-shot data watch on the workgroups node. Does nothing when
     * the watch load() armed is still in place.
     *
     * @return {undefined}
     */
    startWatch() {
        if (this._watchArmed || !this._zkClient) {
            return undefined;
        }
        return this._rearm();
    }

    /**
     * Closes the zookeeper client, when this class opened it
     *
     * @param {Function} [done] - callback
     * @return {undefined}
     */
    stop(done) {
        this._watchArmed = false;
        if (this._zkClient && this._ownsClient) {
            this._zkClient.close();
        }
        this._zkClient = null;
        if (typeof done === 'function') {
            return process.nextTick(done);
        }
        return undefined;
    }

    /**
     * Connects a zookeeper client to the connection string as it is
     * configured. The workgroups path is a path on that client, never a
     * chroot appended to the connection string, so the worker and the
     * cutover tool cannot disagree about which node holds the document.
     *
     * @param {Function} done - callback
     * @return {undefined}
     */
    _setupZookeeper(done) {
        if (this._zkClient) {
            return process.nextTick(done);
        }
        this.log.info('opening zookeeper connection for reading the ' +
            'workgroups configuration', {
            method: 'WorkgroupConfigLoader._setupZookeeper',
            connectionString: this._zkConfig.connectionString,
            zkPath: this._zkPath,
        });
        this._zkClient = new ZookeeperManager(this._zkConfig.connectionString, {
            autoCreateNamespace: this._zkConfig.autoCreateNamespace,
            retries: this._zkConfig.retries,
        }, this.log);
        this._zkClient.once('error', done);
        this._zkClient.once('ready', () => {
            this._zkClient.removeAllListeners('error');
            done();
        });
        return undefined;
    }

    /**
     * Parses a document and checks it against what this worker was deployed
     * to run
     *
     * @param {Buffer|String} data - serialised document
     * @return {Object} { error, doc }
     */
    _checkDocument(data) {
        const { error: parseError, result } = safeJsonParse(data);
        if (parseError) {
            return { error: errors.InternalError.customizeDescription(
                'the workgroups document is not valid JSON: ' +
                `${parseError.message}`) };
        }
        const { error: invalid, value } = this._validateDoc(result);
        if (invalid) {
            return { error: errors.InternalError.customizeDescription(
                `the workgroups document is invalid: ${invalid.message}`) };
        }
        const doc = value;
        if (doc.topic !== this._topic) {
            return { error: errors.InternalError.customizeDescription(
                `the workgroups document is for topic ${doc.topic}, this ` +
                `worker delivers ${this._topic}`) };
        }
        if (!doc.workgroups.some(wg => wg.id === this._workgroupId)) {
            return { error: errors.InternalError.customizeDescription(
                'the workgroups document does not list workgroup ' +
                `${this._workgroupId}, it lists ` +
                `${doc.workgroups.map(wg => wg.id).join(', ')}`) };
        }
        if (this._pinnedGeneration !== undefined &&
            doc.generation !== this._pinnedGeneration) {
            return { error: errors.InternalError.customizeDescription(
                `the workgroups document is at generation ${doc.generation}, ` +
                'this worker is pinned to generation ' +
                `${this._pinnedGeneration}`) };
        }
        return { error: null, doc };
    }

    /**
     * Reads the document from the on-disk cache after zookeeper could not be
     * read. Every check the zookeeper path applies is applied here too: a
     * cache at another generation is an error, never a fallback.
     *
     * @param {String} reason - what failed on the zookeeper path
     * @param {Error} cause - the zookeeper error
     * @param {Function} done - callback: done(err, { doc, source })
     * @return {undefined}
     */
    _loadFromCache(reason, cause, done) {
        this.log.warn(reason, {
            method: 'WorkgroupConfigLoader._loadFromCache',
            zkPath: this._zkPath,
            cachePath: this._cachePath,
            error: cause && (cause.message || cause.name || cause),
        });
        return fs.readFile(this._cachePath, (readErr, data) => {
            if (readErr) {
                return done(errors.InternalError.customizeDescription(
                    `${reason}, and the cache at ${this._cachePath} could ` +
                    `not be read: ${readErr.message}`));
            }
            const { error, doc } = this._checkDocument(data);
            if (error) {
                return done(error);
            }
            if (this._pinnedGeneration === undefined) {
                this.log.warn('no generation is pinned, accepting the cached ' +
                    'workgroups document at the generation it holds', {
                    method: 'WorkgroupConfigLoader._loadFromCache',
                    cachePath: this._cachePath,
                    generation: doc.generation,
                });
            }
            return this._accept(doc, 'cache', done);
        });
    }

    /**
     * Writes the document to the cache, through a temporary file so that
     * several workers on one host never expose a partial file to a reader.
     * A failure is a warning: the cache is a fallback, not a requirement.
     *
     * @param {Object} doc - validated document
     * @param {Function} done - callback, never called with an error
     * @return {undefined}
     */
    _writeCache(doc, done) {
        const tmpPath = `${this._cachePath}.${process.pid}.tmp`;
        const onFailure = err => {
            this.log.warn('could not write the workgroups configuration ' +
                'cache', {
                method: 'WorkgroupConfigLoader._writeCache',
                cachePath: this._cachePath,
                error: err.message,
            });
            return done();
        };
        return fs.writeFile(tmpPath, JSON.stringify(doc), writeErr => {
            if (writeErr) {
                return onFailure(writeErr);
            }
            return fs.rename(tmpPath, this._cachePath, renameErr => {
                if (renameErr) {
                    return onFailure(renameErr);
                }
                return done();
            });
        });
    }

    /**
     * Records the loaded document and reports the generation this worker runs
     *
     * @param {Object} doc - validated document
     * @param {String} source - 'zookeeper' or 'cache'
     * @param {Function} done - callback: done(err, { doc, source })
     * @return {undefined}
     */
    _accept(doc, source, done) {
        this._doc = doc;
        workgroupGeneration.set({
            workgroup: this._workgroupId,
            source,
        }, doc.generation);
        if (doc.generation >= 2 && !doc.barriers) {
            this.log.warn('the workgroups document is past its first ' +
                'generation but carries no barriers, it was not written by ' +
                'the cutover tool', {
                method: 'WorkgroupConfigLoader._accept',
                generation: doc.generation,
            });
        }
        this.log.info('loaded the workgroups configuration', {
            method: 'WorkgroupConfigLoader._accept',
            source,
            zkPath: this._zkPath,
            generation: doc.generation,
            workgroup: this._workgroupId,
            workgroups: doc.workgroups.map(wg => wg.id),
        });
        return done(null, { doc, source });
    }

    /**
     * Handles a fired watch. Zookeeper watches are one-shot, so the only way
     * to keep seeing changes is to read the node again with a new watcher.
     *
     * @param {Object} event - zookeeper watch event
     * @return {undefined}
     */
    _watcher(event) {
        this._watchArmed = false;
        this.log.warn('the workgroups configuration changed in zookeeper', {
            method: 'WorkgroupConfigLoader._watcher',
            zkPath: this._zkPath,
            event,
            generation: this._doc && this._doc.generation,
        });
        workgroupConfigChanges.inc({ workgroup: this._workgroupId });
        return this._rearm();
    }

    /**
     * Reads the node again to place a new watch on it
     *
     * @return {undefined}
     */
    _rearm() {
        return async.retry({
            times: REARM_MAX_ATTEMPTS,
            interval: retryCount =>
                REARM_RETRY_BASE_MS * (2 ** (retryCount - 1)),
        },
        next => this._rearmOnce(next),
        err => {
            if (err) {
                this.log.error('could not re-arm the workgroups ' +
                    'configuration watch, this worker will not see further ' +
                    'changes', {
                    method: 'WorkgroupConfigLoader._rearm',
                    zkPath: this._zkPath,
                    error: err.message || err.name,
                });
            }
            return undefined;
        });
    }

    /**
     * One re-arm attempt. A deleted node cannot carry a data watch, so it is
     * watched through exists() instead and a recreation is still noticed.
     *
     * @param {Function} done - callback
     * @return {undefined}
     */
    _rearmOnce(done) {
        if (!this._zkClient) {
            return process.nextTick(done);
        }
        return this._zkClient.getData(this._zkPath, this._boundWatcher,
            (err, data) => {
                if (err && err.name === 'NO_NODE') {
                    return this._zkClient.exists(this._zkPath,
                        this._boundWatcher, existsErr => {
                            if (existsErr) {
                                return done(existsErr);
                            }
                            this._watchArmed = true;
                            this.log.warn('the workgroups node was deleted, ' +
                                'watching for it to come back', {
                                method: 'WorkgroupConfigLoader._rearmOnce',
                                zkPath: this._zkPath,
                            });
                            return done();
                        });
                }
                if (err) {
                    return done(err);
                }
                this._watchArmed = true;
                this._logObserved(data);
                return done();
            });
    }

    /**
     * Logs how the document now in zookeeper compares with the one this
     * worker runs, then discards it. Rejoining another generation's group is
     * a restart, not a live reconfiguration.
     *
     * @param {Buffer|String} data - serialised document
     * @return {undefined}
     */
    _logObserved(data) {
        const { error, result } = safeJsonParse(data);
        const observed = error ? undefined : result.generation;
        const running = this._doc && this._doc.generation;
        if (observed !== running) {
            this.log.warn('the workgroups configuration in zookeeper is at ' +
                'another generation, this worker keeps serving the one it ' +
                'started with and has to be restarted', {
                method: 'WorkgroupConfigLoader._logObserved',
                zkPath: this._zkPath,
                observedGeneration: observed,
                generation: running,
                workgroup: this._workgroupId,
            });
            return undefined;
        }
        this.log.info('the workgroups configuration changed without changing ' +
            'generation', {
            method: 'WorkgroupConfigLoader._logObserved',
            zkPath: this._zkPath,
            generation: running,
        });
        return undefined;
    }
}

module.exports = WorkgroupConfigLoader;
