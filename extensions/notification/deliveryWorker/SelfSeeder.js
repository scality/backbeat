'use strict';

const os = require('os');
const zookeeper = require('node-zookeeper-client');

const DeliverySeeder = require('./DeliverySeeder');
const { readCommittedState } = require('./seededOffsets');

// how long a worker that lost the lock waits for the holder to publish the
// offsets before giving up and letting assertSeededOffsets refuse the start
const DEFAULT_WAIT_MS = 300000;
const POLL_MS = 2000;

// the generation's group ids end in -gen<n>, which is how the previous
// generation is recovered from the document's previousGroups
const GENERATION_SUFFIX = /-gen(\d+)$/;

/**
 * @param {String} path - workgroups document path
 * @param {Number} generation - generation being seeded
 * @return {String} the ephemeral node exactly one worker of the generation
 *   holds while it seeds
 */
function buildLockPath(path, generation) {
    return `${path}/seed-locks/gen${generation}`;
}

/**
 * The generation whose groups this one is seeded from.
 *
 * The cutover tool records the groups a document replaced, and a group id
 * carries its generation, so the document answers this itself. Without that
 * record the generation before this one is assumed.
 *
 * @param {Object} doc - workgroups document
 * @return {Number} previous generation
 */
function previousGenerationOf(doc) {
    const recorded = (doc.previousGroups || [])
        .map(groupId => GENERATION_SUFFIX.exec(groupId))
        .filter(Boolean)
        .map(match => Number(match[1]));
    if (recorded.length > 0) {
        return Math.max(...recorded);
    }
    return doc.generation - 1;
}

/**
 * @param {Error} err - a zookeeper error
 * @param {String} name - a code name, such as 'NODE_EXISTS'
 * @return {boolean} true when the error carries that code
 */
function isZkCode(err, name) {
    if (!err) {
        return false;
    }
    if (err.name === name) {
        return true;
    }
    return typeof err.getCode === 'function' &&
        err.getCode() === zookeeper.Exception[name];
}

/**
 * @class SelfSeeder
 *
 * @classdesc Seeds a generation's consumer groups from inside the worker
 * that is about to join one of them, so a deployment is a plain container
 * replacement: stop the old containers, write the new configuration, start
 * the new workers. No command runs between the stop and the start.
 *
 * The seeding itself is the DeliverySeeder's, run in process. Exactly one
 * worker of the generation does it, chosen by an ephemeral zookeeper node,
 * and it seeds every group of the document at once; the others wait for
 * their own group's offsets to appear. A worker whose group already has
 * committed offsets touches nothing, which is what makes a restart, a
 * rolling replacement and an operator who seeded ahead with the CLI all
 * safe.
 *
 * Seeding can still fail, for a broker that cannot be reached or a source
 * group that no longer exists. This class reports that and leaves the group
 * unseeded: refusing the start is assertSeededOffsets' job, and its message
 * is the one operators already know.
 */
class SelfSeeder {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.kafkaConfig - kafka configuration object
     * @param {Object} params.zkConfig - the config.zookeeper block
     * @param {Object} params.notifConfig - the extensions.notification block
     * @param {Object} params.doc - the workgroups document this worker runs
     * @param {String} params.groupId - the group this worker will join
     * @param {String} params.workgroupId - this worker's workgroup, for logs
     * @param {Object} params.zkClient - a connected zookeeper client, shared
     *   with the configuration loader
     * @param {Logger} params.logger - werelogs logger
     * @param {Function} [params.seederFactory] - (options) to a DeliverySeeder,
     *   for tests
     * @param {Function} [params.readState] - (params, cb) reading a group's
     *   committed offsets, for tests
     * @param {Function} [params.sleep] - (ms, cb), for tests
     */
    constructor(params) {
        this._kafkaConfig = params.kafkaConfig;
        this._zkConfig = params.zkConfig;
        this._notifConfig = params.notifConfig;
        this._deliveryPoolConfig = params.notifConfig.deliveryPool;
        this._doc = params.doc;
        this._groupId = params.groupId;
        this._workgroupId = params.workgroupId;
        this._zkClient = params.zkClient;
        this._log = params.logger;
        this._zkPath = this._deliveryPoolConfig.workgroups &&
            this._deliveryPoolConfig.workgroups.zookeeperPath;
        this._waitMs = this._deliveryPoolConfig.seedOnStartTimeoutMs ||
            DEFAULT_WAIT_MS;
        this._pollMs = params.pollMs || POLL_MS;
        this._seederFactory = params.seederFactory ||
            (options => new DeliverySeeder({
                kafkaConfig: this._kafkaConfig,
                zkConfig: this._zkConfig,
                notifConfig: this._notifConfig,
                options,
                logger: this._log,
                zkClient: this._zkClient,
            }));
        this._readState = params.readState || readCommittedState;
        this._sleep = params.sleep || ((ms, cb) => setTimeout(cb, ms));
        this._lockPath = this._zkPath ?
            buildLockPath(this._zkPath, this._doc.generation) : null;
        this._holdsLock = false;
    }

    /**
     * Whether a worker in this configuration seeds its own group.
     *
     * Only the internal source: on the delivery topic the first generation
     * starts on an empty topic and later ones are written by the cutover
     * tool, which seeds as part of writing them.
     *
     * @param {Object} notifConfig - the extensions.notification block
     * @return {boolean} true when a start may seed
     */
    static isEnabled(notifConfig) {
        const pool = notifConfig && notifConfig.deliveryPool;
        if (!pool || !pool.enabled) {
            return false;
        }
        if (pool.source === 'delivery') {
            return false;
        }
        if (!pool.workgroups || !pool.workgroups.zookeeperPath) {
            // no document, so no generation and no group to name: the pool
            // runs as the single group it is configured with
            return false;
        }
        return pool.seedOnStart !== false;
    }

    /**
     * Seeds this worker's group when it has no committed offsets.
     *
     * Never calls back with an error: every outcome an operator has to act
     * on is either the group being seeded or assertSeededOffsets refusing
     * the start straight after this.
     *
     * @param {Function} done - callback: done(null, { seeded, reason })
     * @return {undefined}
     */
    seed(done) {
        const deadline = Date.now() + this._waitMs;
        const attempt = () => this._committedState((stateErr, state) => {
            if (stateErr) {
                // the same call assertSeededOffsets is about to make, so it
                // reports the broker failure rather than this
                this._log.warn('could not read the committed offsets before ' +
                    'seeding, leaving the group as it is', {
                    method: 'SelfSeeder.seed',
                    groupId: this._groupId,
                    error: stateErr.description || stateErr.message,
                });
                return done(null, { seeded: false, reason: 'unreadable' });
            }
            if (state.unseeded.length === 0) {
                return done(null, { seeded: false, reason: 'already-seeded' });
            }
            return this._acquire(acquireErr => {
                if (acquireErr) {
                    this._log.warn('could not take the seeding lock, leaving ' +
                        'the group as it is', {
                        method: 'SelfSeeder.seed',
                        lockPath: this._lockPath,
                        error: acquireErr.message || acquireErr.name,
                    });
                    return done(null, { seeded: false, reason: 'no-lock' });
                }
                if (this._holdsLock) {
                    return this._seedUnderLock(done);
                }
                if (Date.now() >= deadline) {
                    this._log.error('another worker has been seeding this ' +
                        'generation for too long', {
                        method: 'SelfSeeder.seed',
                        lockPath: this._lockPath,
                        groupId: this._groupId,
                        waitedMs: this._waitMs,
                    });
                    return done(null, { seeded: false, reason: 'wait-timeout' });
                }
                this._log.info('another worker is seeding this generation, ' +
                    'waiting for the offsets to appear', {
                    method: 'SelfSeeder.seed',
                    lockPath: this._lockPath,
                    groupId: this._groupId,
                    partitions: state.unseeded,
                });
                return this._sleep(this._pollMs, attempt);
            });
        });
        return attempt();
    }

    _committedState(done) {
        return this._readState({
            kafkaConfig: this._kafkaConfig,
            topic: this._notifConfig.topic,
            groupId: this._groupId,
            logger: this._log,
        }, done);
    }

    /**
     * Creates the ephemeral lock node. Sets _holdsLock when this worker is
     * the one that created it; a node another worker holds is not an error.
     *
     * @param {Function} done - callback: done(err)
     * @return {undefined}
     */
    _acquire(done) {
        const payload = Buffer.from(JSON.stringify({
            pid: process.pid,
            host: os.hostname(),
            workgroup: this._workgroupId,
            generation: this._doc.generation,
            at: new Date().toISOString(),
        }));
        return this._zkClient.mkdirp(`${this._zkPath}/seed-locks`, mkErr => {
            if (mkErr && !isZkCode(mkErr, 'NODE_EXISTS')) {
                return done(mkErr);
            }
            return this._zkClient.create(this._lockPath, payload,
                zookeeper.ACL.OPEN_ACL_UNSAFE, zookeeper.CreateMode.EPHEMERAL,
                createErr => {
                    if (createErr) {
                        return isZkCode(createErr, 'NODE_EXISTS') ? done() :
                            done(createErr);
                    }
                    this._holdsLock = true;
                    return done();
                });
        });
    }

    _release(done) {
        if (!this._holdsLock) {
            return process.nextTick(done);
        }
        return this._zkClient.remove(this._lockPath, -1, err => {
            this._holdsLock = false;
            if (err && !isZkCode(err, 'NO_NODE')) {
                // the node is ephemeral, so the session expiring clears it
                this._log.warn('could not remove the seeding lock', {
                    method: 'SelfSeeder._release',
                    lockPath: this._lockPath,
                    error: err.message || err.name,
                });
            }
            return done();
        });
    }

    /**
     * Seeds every group of the document, holding the lock.
     *
     * @param {Function} done - callback: done(null, result)
     * @return {undefined}
     */
    _seedUnderLock(done) {
        const finish = result => this._release(() => done(null, result));
        // the state was read before the lock was taken, so re-read it now:
        // another worker may have seeded and released in between, and
        // committing a group a live worker has already joined fails
        return this._committedState((stateErr, state) => {
            if (!stateErr && state.unseeded.length === 0) {
                return finish({ seeded: false, reason: 'already-seeded' });
            }
            return this._runSeeder((err, seedResult) => {
                if (err) {
                    this._log.error('this worker could not seed its ' +
                        'generation, the start will be refused', {
                        method: 'SelfSeeder._seedUnderLock',
                        groupId: this._groupId,
                        generation: this._doc.generation,
                        error: err.description || err.message,
                    });
                    return finish({ seeded: false, reason: 'seed-failed',
                        error: err });
                }
                this._log.info('this generation seeded itself: no seed ' +
                    'command ran between the stop and this start', {
                    method: 'SelfSeeder._seedUnderLock',
                    generation: this._doc.generation,
                    groupId: this._groupId,
                    groups: seedResult.groups.map(g => g.groupId),
                    watermarksPath: seedResult.watermarksPath,
                });
                return finish({ seeded: true, reason: 'seeded',
                    result: seedResult });
            });
        });
    }

    /**
     * Runs the seeding: from the per-destination processor groups for the
     * first generation, from the previous generation's groups after that.
     *
     * @param {Function} done - callback: done(err, result)
     * @return {undefined}
     */
    _runSeeder(done) {
        const generation = this._doc.generation;
        const from = previousGenerationOf(this._doc);
        const fromProcessors = generation <= 1 || from < 1;
        const options = fromProcessors ? { generation } : { from, to: generation };
        this._log.info(fromProcessors ?
            'seeding this generation from the queue processor groups' :
            'seeding this generation from the previous generation\'s groups', {
            method: 'SelfSeeder._runSeeder',
            generation,
            from: fromProcessors ? undefined : from,
        });
        const seeder = this._seederFactory(options);
        const method = fromProcessors ? 'seedFromProcessors' :
            'seedFromGeneration';
        return seeder[method]((err, result) =>
            seeder.close(() => done(err, result)));
    }
}

module.exports = SelfSeeder;
module.exports.buildLockPath = buildLockPath;
module.exports.previousGenerationOf = previousGenerationOf;
