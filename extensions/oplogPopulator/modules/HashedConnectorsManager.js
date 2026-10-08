const joi = require('joi');
const async = require('async');
const util = require('util');

const constants = require('../constants');
const KafkaConnectWrapper = require('../../../lib/wrappers/KafkaConnectWrapper');
const HashedPipelineFactory = require('../pipeline/HashedPipelineFactory');
const { scheduleExclusiveJob } = require('../../../lib/util/scheduleExclusiveJob');
const {
    connectorsManagerParamsJoi,
    buildConnectorConfig,
    newPartitionName,
    getFailedTasks,
    isConnectorFailed,
} = require('./connectorConfig');
const { ownedConnectorPrefix, parseHashedConnectorName, hashedConnectorName } = require('./connectorNaming');
const {
    timestampFromSourceOffset,
    timestampFromStartupTime,
    formatStartupTime,
    minTimestamp,
} = require('./resumeToken');

const paramsJoi = connectorsManagerParamsJoi.keys({
    pipelineFactory: joi.object().instance(HashedPipelineFactory).required(),
    getClusterTime: joi.func().required(),
});

const eachLimit = util.promisify(async.eachLimit);
const mapLimit = util.promisify(async.mapLimit);
// bounds parallel kafka connect requests: there may be thousands of per-bucket
// connectors to migrate, and some calls are expensive (offsets reads make the
// worker read its whole offsets topic)
const kafkaConnectConcurrency = 10;

const startupTimeKey = 'startup.mode.timestamp.start.at.operation.time';
const partitionNameKey = 'offset.partition.name';
// Configuration fields specific to one connector instance, or covered by the
// connector name, that must not be compared to (or replaced by) the desired ones
const unmanagedConfigKeys = new Set(['name', 'pipeline', partitionNameKey, startupTimeKey]);

// built on the global setTimeout (not timers/promises) so that tests can
// fake it
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * @class HashedConnectorsManager
 *
 * @classdesc Keeps a fixed number N of hashed connectors: connector k
 * ingests the bucket collections whose name hashes to k modulo N. Any other
 * connector (per-bucket, other N, other pipeline) is migrated to them
 * without losing events: the old connectors are paused, and only deleted
 * once the hashed connectors exist, starting from the old connectors'
 * committed offsets.
 */
class HashedConnectorsManager {
    /**
     * @constructor
     * @param {Object} params params
     * @param {number} params.nbConnectors number of hashed connectors
     * @param {string} params.database MongoDB database to watch
     * @param {string} params.mongoUrl MongoDB connection url
     * @param {string} params.oplogTopic topic to produce to
     * @param {string} params.cronRule connector updates cron rule
     * @param {string} [params.prefix] connector name prefix
     * @param {number} params.heartbeatIntervalMs connector heartbeat interval
     * @param {string} params.kafkaConnectHost kafka connect host
     * @param {number} params.kafkaConnectPort kafka connect port
     * @param {HashedPipelineFactory} params.pipelineFactory pipeline factory
     * @param {OplogPopulatorMetrics} params.metricsHandler metrics handler
     * @param {Function} params.getClusterTime async function returning the
     * current MongoDB cluster time, `{ t, i }`
     * @param {Logger} params.logger logger
     */
    constructor(params) {
        joi.attempt(params, paramsJoi);
        this._nbConnectors = params.nbConnectors;
        this._cronRule = params.cronRule;
        this._prefix = params.prefix || '';
        this._metricsHandler = params.metricsHandler;
        this._getClusterTime = params.getClusterTime;
        this._logger = params.logger;
        this._kafkaConnect = new KafkaConnectWrapper({
            kafkaConnectHost: params.kafkaConnectHost,
            kafkaConnectPort: params.kafkaConnectPort,
            logger: this._logger,
        });
        this._desired = [...Array(this._nbConnectors).keys()].map(index => {
            const pipeline = params.pipelineFactory.getPipeline({ nbConnectors: this._nbConnectors, index });
            const name = hashedConnectorName({
                prefix: this._prefix,
                generation: constants.hashedPipelineGeneration,
                nbConnectors: this._nbConnectors,
                index,
                pipeline,
            });
            const config = buildConnectorConfig({
                name,
                database: params.database,
                mongoUrl: params.mongoUrl,
                oplogTopic: params.oplogTopic,
                heartbeatIntervalMs: params.heartbeatIntervalMs,
            });
            return { index, name, config: { ...config, pipeline } };
        });
        // when each outdated connector was first seen paused, to only wait
        // for the remaining offset flush delay when retrying a migration
        this._pausedAt = new Map();
    }

    /**
     * Schedules the periodic reconciliation of the connectors
     * @returns {undefined}
     */
    start() {
        scheduleExclusiveJob(this._cronRule, () => this.reconcile(), this._logger);
    }

    /**
     * Brings the connectors to the desired hashed connectors
     * @returns {Promise<undefined>} undefined
     */
    async reconcile() {
        const connectors = await this._kafkaConnect.getConnectorsWithStatus();
        const plan = this._classify(connectors);
        this._observe(plan);
        if (plan.newer.length > 0) {
            // Some newer generation connectors exist: stop and avoid touching anything.
            // The newer generation populator will handle the upgrade / deleting our connectors.
            this._logger.warn('connectors of a newer oplog populator found, leaving connectors alone', {
                method: 'HashedConnectorsManager.reconcile',
                connectors: plan.newer.map(c => c.name),
            });
            return;
        }
        await this._checkConnectorStates(plan.current);
        await this._checkConnectorConfigs(plan.current);
        if (plan.missing.length === 0) {
            // All our new connectors are present: delete any outstanding old connector and exit.
            await this._deleteConnectors(plan.outdated.map(c => c.name));
            return;
        }
        // We are missing some new connectors.
        if (plan.outdated.length === 0) {
            // All previous connectors are gone, nothing to migrate:
            // just create the missing new connectors and exit.
            // (Typically, new installs.)
            const now = await this._getClusterTime();
            await this._createConnectors(plan.missing, new Map(), now);
            return;
        }
        // Some old connectors exist, and some new connectors need to be created.
        // Prepare a rolling migration.
        await this._migrateConnectors(plan);
    }

    /**
     * Sorts the connectors listed by kafka connect against the desired ones.
     * @param {Object} connectors connectors as listed by
     * `KafkaConnectWrapper.getConnectorsWithStatus()`
     * @returns {Object} `{ newer, current, outdated, missing }`: `newer`,
     * `current` and `outdated` hold `{ name, config, status, parsed }`
     * (`current` also holds `desired`), `missing` holds desired connectors
     */
    _classify(connectors) {
        const desiredByName = new Map(this._desired.map(d => [d.name, d]));
        const plan = { newer: [], current: [], outdated: [], missing: [] };
        Object.entries(connectors).forEach(([name, { info, status }]) => {
            if (!name.startsWith(ownedConnectorPrefix(this._prefix))) {
                return;
            }
            const parsed = parseHashedConnectorName(this._prefix, name);
            const connector = { name, config: info?.config || {}, status, parsed };
            if (parsed && parsed.generation > constants.hashedPipelineGeneration) {
                plan.newer.push(connector);
            } else if (desiredByName.has(name)) {
                plan.current.push({ ...connector, desired: desiredByName.get(name) });
            } else {
                plan.outdated.push(connector);
            }
        });
        plan.missing = this._desired.filter(d => !(d.name in connectors));
        return plan;
    }

    /**
     * Updates the metrics describing the connectors
     * @param {Object} plan classified connectors
     * @returns {undefined}
     */
    _observe(plan) {
        const states = { running: 0, paused: 0, failed: 0, edited: 0 };
        plan.current.forEach(c => {
            states[this._connectorState(c.status)] += 1;
            if (this._isPipelineEdited(c)) {
                states.edited += 1;
            }
        });
        this._metricsHandler.onHashedConnectorsObserved({
            connectors: plan.current.length + plan.outdated.length + plan.newer.length,
            states,
            outdated: plan.outdated.length,
            newer: plan.newer.length,
        });
    }

    /**
     * Restarts failed hashed connectors; paused ones are only reported, never
     * resumed
     * @param {Object[]} current current hashed connectors
     * @returns {Promise<undefined>} undefined
     */
    async _checkConnectorStates(current) {
        await eachLimit(current, kafkaConnectConcurrency, async connector => {
            const { name } = connector;
            try {
                const state = this._connectorState(connector.status);
                if (state === 'failed') {
                    getFailedTasks(connector.status).forEach(task => this._logger.error('connector task failed', {
                        method: 'HashedConnectorsManager._checkConnectorStates',
                        connector: name,
                        taskId: task.id,
                        error: task.trace,
                    }));
                    await this._kafkaConnect.restartConnector(name, true, true);
                    this._metricsHandler.onConnectorRestart({ name });
                } else if (state === 'paused') {
                    this._logger.warn('connector is paused, not resuming it', {
                        method: 'HashedConnectorsManager._checkConnectorStates',
                        connector: name,
                    });
                }
            } catch (err) {
                this._logger.error('could not restart connector', {
                    method: 'HashedConnectorsManager._checkConnectorStates',
                    connector: name,
                    error: err.description || err.message,
                });
            }
        });
    }

    /**
     * Updates the configuration of hashed connectors that drifted from the
     * desired one; edited pipelines are only reported
     * @param {Object[]} current current hashed connectors
     * @returns {Promise<undefined>} undefined
     */
    async _checkConnectorConfigs(current) {
        await eachLimit(current, kafkaConnectConcurrency, async connector => {
            const { name } = connector;
            try {
                if (this._isPipelineEdited(connector)) {
                    this._logger.warn('connector pipeline was edited', {
                        method: 'HashedConnectorsManager._checkConnectorConfigs',
                        connector: name,
                    });
                }
                const drifted = this._configDrift(connector.config, connector.desired.config);
                if (drifted.length > 0) {
                    const update = Object.fromEntries(drifted.map(key => [key, connector.desired.config[key]]));
                    await this._kafkaConnect.updateConnectorConfig(name, { ...connector.config, ...update });
                    // values are not logged, the connection uri holds credentials
                    this._logger.info('updated connector configuration', {
                        method: 'HashedConnectorsManager._checkConnectorConfigs',
                        connector: name,
                        fields: drifted,
                    });
                }
            } catch (err) {
                this._logger.error('could not update connector configuration', {
                    method: 'HashedConnectorsManager._checkConnectorConfigs',
                    connector: name,
                    error: err.description || err.message,
                });
            }
        });
    }

    /**
     * Replaces the outdated connectors by the missing hashed connectors:
     * pauses them, creates the hashed connectors from their last committed offsets, then deletes
     * them. Returns early, to retry on a later reconciliation, while Kafka
     * Connect cannot serve offsets or a paused connector keeps running.
     * @param {Object} plan classified connectors
     * @returns {Promise<undefined>} undefined
     */
    async _migrateConnectors({ outdated, missing }) {
        this._logger.info('migrating to hashed connectors', {
            method: 'HashedConnectorsManager._migrateConnectors',
            outdated: outdated.map(c => c.name),
            missing: missing.map(d => d.name),
        });
        this._metricsHandler.onMigrationInProgress(true);
        try {
            // This is a rolling update of connectors from a previous generation, to our current generation.
            // The general idea is:
            // - pause all previous connectors, noting their offset (we will turn offsets into oplog pipeline time)
            // - (wait for the previous connectors to really be paused, it takes a little while)
            // - create the new connectors from these offsets
            // - delete the previous connectors
            // The function is stateless-ish and idempotent and can exit early to rerun later and finish migrating.

            // One complexity we have to deal with is the previous pod/generation possibly
            // messing with our connectors by deleting/recreating them at any time.
            // This is why there are extra get/get offset calls.

            // First, ensure Kafka Connect >= 3.5, supporting getting offsets, was rolled out; otherwise exit/wait.
            // Also, get the previous connector offsets before pausing and waiting below
            // (in case they are deleted while waiting).
            const firstReadAt = Date.now();
            const beforePause = await this._readOffsets(outdated);
            if (!beforePause) {
                return;
            }

            // Pause all connectors (dropping any that were deleted before us).
            const gone = new Set();
            await eachLimit(beforePause, kafkaConnectConcurrency, async connector => {
                try {
                    await this._kafkaConnect.pauseConnector(connector.name);
                    if (!this._pausedAt.has(connector.name)) {
                        this._pausedAt.set(connector.name, Date.now());
                    }
                } catch (err) {
                    if (err.statusCode !== 404) {
                        throw err;
                    }
                    gone.add(connector.name);
                }
            });
            const paused = beforePause.filter(c => !gone.has(c.name));
            if (!await this._waitStopped(paused)) {
                return;
            }

            // Wait for the connectors to be really paused and flushed.
            // That's min(pause time) + flush interval.
            const pausedAt = Math.min(...paused.map(c => this._pausedAt.get(c.name)));
            const flushRemaining = constants.kafkaConnectOffsetFlushIntervalMs - (Date.now() - pausedAt);
            if (flushRemaining > 0) {
                await sleep(flushRemaining);
            }

            // Read offsets again; they may have moved while we were waiting for the flushes.
            const afterPause = await this._readOffsets(paused);
            if (!afterPause) {
                return;
            }

            // We are about to create the new connectors, but we need their start oplog times.
            // We take, in order of preference:
            // - their offset after the pause, or
            // - their offset before the pause, if deleted since, or
            // - some conservative fallback, if no offset at all,
            //   based on when we paused it, or when we ever read it first
            const now = await this._getClusterTime();
            const offsetsAfterPause = new Map(afterPause.map(c => [c.name, c.offsets]));
            const outdatedWithTimes = beforePause.map(connector => {
                const offsets = offsetsAfterPause.get(connector.name) || connector.offsets;
                const time = this._connectorOplogTime(offsets, connector.config);
                if (time) {
                    return { ...connector, time };
                }
                this._logger.warn('unknown connector oplog time, assuming it never ran long', {
                    method: 'HashedConnectorsManager._migrateConnectors',
                    connector: connector.name,
                });
                // No offset means no task has flushed yet, so it started at most a flush interval before
                // it stopped being observed (we take some additional margin on top).
                const stoppedAt = this._pausedAt.get(connector.name) ?? firstReadAt;
                const secondsSinceStopped = Math.ceil((Date.now() - stoppedAt) / 1000);
                const marginSeconds = Math.ceil(2 * constants.kafkaConnectOffsetFlushIntervalMs / 1000);
                return { ...connector, time: { t: now.t - secondsSinceStopped - marginSeconds, i: 0 } };
            });
            // Start each new connector at the earliest time of the outdated connectors covering its collections.
            const startTimes = this._startTimes(missing, outdatedWithTimes);

            // Create our new connectors, now that we know when to start them off from.
            await this._createConnectors(missing, startTimes, now);

            // New connectors are created, we can now delete the previous, paused, connectors.
            await this._deleteConnectors(afterPause.map(c => c.name));

            const earliest = minTimestamp([...startTimes.values()].filter(t => t !== null)) || now;
            this._metricsHandler.onMigration(true, now.t - earliest.t);
        } catch (err) {
            this._metricsHandler.onMigration(false);
            throw err;
        } finally {
            this._metricsHandler.onMigrationInProgress(false);
        }
    }

    /**
     * Reads the committed offsets of connectors, leaving out connectors that
     * no longer exist
     * @param {Object[]} connectors connectors
     * @returns {Promise<Object[]|null>} connectors with their `offsets`, or
     * null if the offsets API is not available (Kafka Connect < 3.5)
     */
    async _readOffsets(connectors) {
        const results = await mapLimit(connectors, kafkaConnectConcurrency, async connector => {
            try {
                return { ...connector, offsets: await this._kafkaConnect.getConnectorOffsets(connector.name) };
            } catch (err) {
                if (err.statusCode === 404) {
                    return { ...connector, offsets: null };
                }
                throw err;
            }
        });
        const unread = results.filter(c => c.offsets === null).map(c => c.name);
        if (unread.length > 0) {
            // fetch connectors to tell apart old Kafka Connect without offset support,
            // from connectors having really disappeared (the plain listing, as the
            // expanded one may leave out existing connectors).
            // if any connector exists while its offsets request 404'd, we stop and
            // wait for the new Kafka Connect to roll out.
            const existing = new Set(await this._kafkaConnect.getConnectors());
            if (unread.some(name => existing.has(name))) {
                this._logger.warn('kafka connect offsets API unavailable, retrying later', {
                    method: 'HashedConnectorsManager._readOffsets',
                });
                return null;
            }
        }
        return results.filter(c => c.offsets !== null);
    }

    /**
     * Waits for the tasks of paused connectors to stop producing. Unassigned
     * tasks do not run, and resume as paused when assigned again.
     * @param {Object[]} connectors paused connectors
     * @returns {Promise<boolean>} false if some still run
     */
    async _waitStopped(connectors) {
        const stoppedStates = ['PAUSED', 'FAILED', 'UNASSIGNED'];
        const deadline = Date.now() + constants.pauseTimeoutMs;
        while (Date.now() < deadline) {
            const listed = await this._kafkaConnect.getConnectorsWithStatus();
            const allStopped = connectors.every(c => {
                const status = listed[c.name]?.status;
                // gone, or its status could not be read: either way we paused it
                if (!status) {
                    return true;
                }
                return stoppedStates.includes(status.connector?.state) &&
                    (status.tasks || []).every(task => stoppedStates.includes(task.state));
            });
            if (allStopped) {
                return true;
            }
            await sleep(constants.pausePollIntervalMs);
        }
        this._logger.warn('paused connectors still running, retrying later', {
            method: 'HashedConnectorsManager._waitStopped',
        });
        return false;
    }

    /**
     * Creates hashed connectors
     * @param {Object[]} connectors desired connectors
     * @param {Map<number, Object|null>} startTimes start oplog time of each
     * connector index
     * @param {Object} now current cluster time, start time of connectors
     * without one
     * @returns {Promise<undefined>} undefined
     */
    async _createConnectors(connectors, startTimes, now) {
        await eachLimit(connectors, kafkaConnectConcurrency, async ({ index, name, config }) => {
            const start = startTimes.get(index) || now;
            try {
                await this._kafkaConnect.createConnector({
                    name,
                    config: {
                        ...config,
                        [partitionNameKey]: newPartitionName(),
                        [startupTimeKey]: formatStartupTime(start),
                    },
                });
            } catch (err) {
                if (err.statusCode !== 409) {
                    throw err;
                }
                // left out of the listing, it will be reconciled next time
                this._logger.info('connector already exists', {
                    method: 'HashedConnectorsManager._createConnectors',
                    connector: name,
                });
                return;
            }
            this._logger.info('created connector', {
                method: 'HashedConnectorsManager._createConnectors',
                connector: name,
                start,
            });
        });
    }

    /**
     * Deletes connectors
     * @param {string[]} names connector names
     * @returns {Promise<undefined>} undefined
     */
    async _deleteConnectors(names) {
        await eachLimit(names, kafkaConnectConcurrency, async name => {
            try {
                await this._kafkaConnect.deleteConnector(name);
            } catch (err) {
                if (err.statusCode !== 404) {
                    throw err;
                }
            }
            this._pausedAt.delete(name);
            this._logger.info('deleted connector', {
                method: 'HashedConnectorsManager._deleteConnectors',
                connector: name,
            });
        });
    }

    /**
     * State of a hashed connector
     * @param {Object} status connector status
     * @returns {string} one of `failed`, `paused`, `running`
     */
    _connectorState(status) {
        if (isConnectorFailed(status)) {
            return 'failed';
        }
        return status?.connector?.state === 'PAUSED' ? 'paused' : 'running';
    }

    /**
     * Whether a hashed connector pipeline was edited since its creation: its name embeds
     * the hash of the pipeline it was created with
     * @param {Object} connector current connector, `{ name, config, parsed }`
     * @returns {boolean} true if the pipeline no longer matches the name
     */
    _isPipelineEdited(connector) {
        const expectedName = hashedConnectorName({
            prefix: this._prefix,
            ...connector.parsed,
            pipeline: connector.config.pipeline || '',
        });
        return expectedName !== connector.name;
    }

    /**
     * Lists the managed configuration fields whose live value differs from the
     * desired one. Kafka connect stores every value as a string.
     * @param {Object} live live connector configuration
     * @param {Object} desired desired connector configuration
     * @returns {string[]} drifted field names
     */
    _configDrift(live, desired) {
        return Object.keys(desired).filter(key =>
            !unmanagedConfigKeys.has(key) && String(live[key]) !== String(desired[key]));
    }

    /**
     * Oplog time reached by a connector: its committed offset, or else the time
     * it was configured to start at, which it never went past without
     * committing
     * @param {Object[]} offsets offsets of the connector, as returned by
     * `KafkaConnectWrapper.getConnectorOffsets()`
     * @param {Object} config connector configuration
     * @returns {Object|null} `{ t, i }`, or null if unknown
     */
    _connectorOplogTime(offsets, config) {
        const partitionName = config[partitionNameKey];
        // offsets of previous incarnations (other partition names) are stale
        const current = offsets.filter(o => !partitionName || o.partition?.ns === partitionName);
        const committed = minTimestamp(current
            .map(o => timestampFromSourceOffset(o.offset))
            .filter(ts => ts !== null));
        return committed || timestampFromStartupTime(config[startupTimeKey]);
    }

    /**
     * Start oplog time of each missing connector: the earliest time among the
     * outdated connectors covering its collections. A hashed connector of
     * the same generation and number of connectors covers exactly the same
     * collections as the desired connector of the same index, and none of
     * the others'; another generation may partition collections differently.
     * @param {Object[]} missing missing desired connectors, `{ index }`
     * @param {Object[]} outdated outdated connectors with their oplog `time`
     * @returns {Map<number, Object|null>} connector index to `{ t, i }`, null
     * when no outdated connector covers the connector
     */
    _startTimes(missing, outdated) {
        const overlaps = (connector, index) => !connector.parsed ||
            connector.parsed.generation !== constants.hashedPipelineGeneration ||
            connector.parsed.nbConnectors !== this._nbConnectors || connector.parsed.index === index;
        return new Map(missing.map(({ index }) => [index, minTimestamp(
            outdated.filter(f => overlaps(f, index)).map(f => f.time))]));
    }
}

module.exports = HashedConnectorsManager;
