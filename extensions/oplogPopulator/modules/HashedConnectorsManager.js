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
    timestampFromConnectorOffset,
    timestampFromConnectorStartupTime,
    connectorStartupTime,
    minTimestamp,
} = require('./resumeToken');

const paramsJoi = connectorsManagerParamsJoi.keys({
    nbConnectors: joi.number().integer().min(1).required(),
    pipelineFactory: joi.object().instance(HashedPipelineFactory).required(),
    getClusterTime: joi.func().required(),
});

const eachLimit = util.promisify(async.eachLimit);
const mapLimit = util.promisify(async.mapLimit);
const { kafkaConnectConcurrency, connectorPartitionNameKey, connectorStartupTimeKey } = constants;

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
 * without losing events: the old connectors are stopped, which commits
 * their last offsets, and only deleted once the hashed connectors exist,
 * starting from those offsets.
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
        this._connectorSpecs = [...Array(this._nbConnectors).keys()].map(index => {
            const pipeline = params.pipelineFactory.getPipeline({ nbConnectors: this._nbConnectors, index });
            const name = hashedConnectorName({
                prefix: this._prefix,
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
        this._connectorSpecsByName = new Map(this._connectorSpecs.map(spec => [spec.name, spec]));
    }

    /**
     * Schedules the periodic reconciliation of the connectors
     * @returns {undefined}
     */
    start() {
        this._metricsHandler.onHashedConnectorsStarted();
        scheduleExclusiveJob(this._cronRule, () => this.reconcile(), this._logger, 'HashedConnectorsManager.reconcile');
    }

    /**
     * Brings the connectors to their specs: fixes the current connectors,
     * creates the missing ones (when migrating, from the oplog time reached by
     * the outdated ones), then deletes the outdated ones
     * @returns {Promise<undefined>} undefined
     */
    async reconcile() {
        const connectors = await this._kafkaConnect.getConnectorsWithStatus();
        const { current, outdated, missing } = this._classify(connectors);
        this._observe(current, outdated);
        await this._checkConnectors(current);
        const migrating = missing.length > 0 && outdated.length > 0;
        try {
            let startTimes = new Map();
            if (migrating) {
                startTimes = await this._stopOutdatedConnectors(outdated, missing);
                if (!startTimes) {
                    return;
                }
            }
            // any failed creation throws, keeping the outdated connectors
            await this._createConnectors(missing, startTimes);
            await this._deleteConnectors(outdated.map(c => c.name));
        } catch (err) {
            if (migrating) {
                this._metricsHandler.onMigration(false);
            }
            throw err;
        }
        if (migrating) {
            this._metricsHandler.onMigration(true);
        }
    }

    /**
     * Sorts the connectors listed by kafka connect against the connector specs
     * @param {Object} connectors connectors as listed by
     * `KafkaConnectWrapper.getConnectorsWithStatus()`
     * @returns {Object} `{ current, outdated, missing }`: `current` holds
     * `{ name, config, status, spec }`, `outdated` holds
     * `{ name, config, status, parsed }`, `missing` holds connector specs
     */
    _classify(connectors) {
        const plan = { current: [], outdated: [], missing: [] };
        Object.entries(connectors).forEach(([name, { info, status }]) => {
            // a connector whose configuration could not be read is left alone
            if (!name.startsWith(ownedConnectorPrefix(this._prefix)) || !info?.config) {
                return;
            }
            const connector = { name, config: info.config, status };
            const spec = this._connectorSpecsByName.get(name);
            if (spec) {
                plan.current.push({ ...connector, spec });
            } else {
                plan.outdated.push({ ...connector, parsed: parseHashedConnectorName(this._prefix, name) });
            }
        });
        plan.missing = this._connectorSpecs.filter(spec => !(spec.name in connectors));
        return plan;
    }

    /**
     * Updates the metrics describing the connectors
     * @param {Object[]} current current hashed connectors
     * @param {Object[]} outdated outdated connectors
     * @returns {undefined}
     */
    _observe(current, outdated) {
        const states = { running: 0, paused: 0, failed: 0, outdated: outdated.length };
        current.forEach(c => {
            states[this._connectorState(c.status)] += 1;
            this._metricsHandler.onHashedConnectorPipeline(c.name, c.config.pipeline);
        });
        this._metricsHandler.onHashedConnectorsObserved(states);
    }

    /**
     * Fixes the current hashed connectors: updates the managed configuration
     * fields that drifted (which restarts its tasks), or else restarts a
     * failed connector; a paused or stopped one is only reported, never
     * resumed
     * @param {Object[]} current current hashed connectors
     * @returns {Promise<undefined>} undefined
     */
    async _checkConnectors(current) {
        await eachLimit(current, kafkaConnectConcurrency, async ({ name, config, status, spec }) => {
            try {
                const drifted = this._configDrift(config, spec.config);
                const state = this._connectorState(status);
                if (drifted.length > 0) {
                    const update = { ...config, ...Object.fromEntries(drifted.map(key => [key, spec.config[key]])) };
                    await this._applyChange(() => this._kafkaConnect.updateConnectorConfig(name, update), update);
                    // values are not logged, the connection uri holds credentials
                    this._logger.info('updated connector configuration', {
                        method: 'HashedConnectorsManager._checkConnectors',
                        connector: name,
                        fields: drifted,
                    });
                } else if (state === 'failed') {
                    getFailedTasks(status).forEach(task => this._logger.error('connector task failed', {
                        method: 'HashedConnectorsManager._checkConnectors',
                        connector: name,
                        taskId: task.id,
                        error: task.trace,
                    }));
                    await this._kafkaConnect.restartConnector(name, true, true);
                    this._metricsHandler.onConnectorRestart({ name });
                } else if (state === 'paused') {
                    this._logger.warn('connector is paused or stopped, not resuming it', {
                        method: 'HashedConnectorsManager._checkConnectors',
                        connector: name,
                    });
                }
            } catch (err) {
                this._logger.error('could not update or restart connector', {
                    method: 'HashedConnectorsManager._checkConnectors',
                    connector: name,
                    error: err.description || err.message,
                });
            }
        });
    }

    /**
     * Stops the outdated connectors, which commits their last offsets, and
     * computes the start time of each missing connector from them. They stay
     * stopped until deleted, or until a later reconciliation retries.
     * @param {Object[]} outdated outdated connectors
     * @param {Object[]} missing missing connector specs
     * @returns {Promise<Map|null>} start oplog time of each missing connector
     * index, or null to retry on a later reconciliation
     */
    async _stopOutdatedConnectors(outdated, missing) {
        this._logger.info('migrating to hashed connectors', {
            method: 'HashedConnectorsManager._stopOutdatedConnectors',
            outdated: outdated.map(c => c.name),
            missing: missing.map(spec => spec.name),
        });
        const stopped = await mapLimit(outdated, kafkaConnectConcurrency, async c => this._stopConnector(c));
        const retries = stopped.filter(result => result?.retry);
        if (retries.length > 0) {
            this._logger.warn('outdated connectors not ready to migrate, retrying later', {
                method: 'HashedConnectorsManager._stopOutdatedConnectors',
                reasons: [...new Set(retries.map(result => result.retry))],
            });
            return null;
        }
        const now = await this._getClusterTime();
        const outdatedTimes = stopped.filter(result => result).map(({ connector, offsets, stoppedAt }) => {
            const time = this._connectorOplogTime(offsets, connector.config);
            if (time) {
                return { parsed: connector.parsed, time };
            }
            this._logger.warn('unknown connector oplog time, assuming it never ran long', {
                method: 'HashedConnectorsManager._stopOutdatedConnectors',
                connector: connector.name,
            });
            // without any offset it never committed, so it started at most a
            // flush interval before it stopped (with some margin on top)
            const secondsSinceStopped = Math.ceil((Date.now() - stoppedAt) / 1000);
            const marginSeconds = Math.ceil(2 * constants.kafkaConnectOffsetFlushIntervalMs / 1000);
            return { parsed: connector.parsed, time: { t: now.t - secondsSinceStopped - marginSeconds, i: 0 } };
        });
        return this._startTimes(missing, outdatedTimes);
    }

    /**
     * Stops an outdated connector, and reads the offsets it committed
     * @param {Object} connector outdated connector
     * @returns {Promise<Object|undefined>} `{ connector, offsets, stoppedAt }`,
     * `{ retry: reason }` to retry later, or undefined if the connector was
     * deleted before its offsets could be read
     */
    async _stopConnector(connector) {
        const { name } = connector;
        // read first: this tells whether kafka connect serves offsets, and
        // remains the best known position if the connector is deleted meanwhile
        const before = await this._readOffsets(name);
        if (!Array.isArray(before)) {
            return before;
        }
        try {
            await this._kafkaConnect.stopConnector(name);
        } catch (err) {
            if (err.statusCode !== 404) {
                throw err;
            }
            return { connector, offsets: before, stoppedAt: Date.now() };
        }
        if (!await this._waitStopped(name)) {
            return { retry: 'connector tasks still running' };
        }
        // stopped tasks commit their final offsets before kafka connect
        // serves this read
        const after = await this._readOffsets(name);
        return { connector, offsets: Array.isArray(after) ? after : before, stoppedAt: Date.now() };
    }

    /**
     * Reads the offsets committed by a connector
     * @param {string} name connector name
     * @returns {Promise<Object[]|Object|undefined>} offsets, `{ retry: reason }`
     * if kafka connect cannot serve offsets (< 3.5), or undefined if the
     * connector does not exist
     */
    async _readOffsets(name) {
        try {
            return await this._kafkaConnect.getConnectorOffsets(name);
        } catch (err) {
            if (err.statusCode !== 404) {
                throw err;
            }
        }
        // the plain listing, as the expanded one may leave out existing connectors
        if ((await this._kafkaConnect.getConnectors()).includes(name)) {
            return { retry: 'kafka connect offsets API unavailable' };
        }
        return undefined;
    }

    /**
     * Waits for a stopped connector's tasks to shut down
     * @param {string} name connector name
     * @returns {Promise<boolean>} false if they did not within the timeout
     */
    async _waitStopped(name) {
        const deadline = Date.now() + constants.stopTimeoutMs;
        while (Date.now() < deadline) {
            let status;
            try {
                status = await this._kafkaConnect.getConnectorStatus(name);
            } catch (err) {
                if (err.statusCode === 404) {
                    return true;
                }
                throw err;
            }
            if (status.connector?.state === 'STOPPED' && !status.tasks?.length) {
                return true;
            }
            await sleep(constants.stopPollIntervalMs);
        }
        return false;
    }

    /**
     * Creates hashed connectors
     * @param {Object[]} specs specs of the connectors to create
     * @param {Map<number, Object|null>} startTimes start oplog time of each
     * connector index, the current cluster time if none
     * @returns {Promise<undefined>} undefined
     */
    async _createConnectors(specs, startTimes) {
        if (specs.length === 0) {
            return;
        }
        const now = await this._getClusterTime();
        await eachLimit(specs, kafkaConnectConcurrency, async ({ index, name, config }) => {
            const start = startTimes.get(index) || now;
            const connectorConfig = {
                ...config,
                [connectorPartitionNameKey]: newPartitionName(),
                [connectorStartupTimeKey]: connectorStartupTime(start),
            };
            // a 409 also answers requests during a worker rebalance: failing
            // here keeps the outdated connectors until the next reconciliation
            await this._applyChange(
                () => this._kafkaConnect.createConnector({ name, config: connectorConfig }), connectorConfig);
            this._logger.info('created connector', {
                method: 'HashedConnectorsManager._createConnectors',
                connector: name,
                start,
            });
        });
    }

    /**
     * Sends a connector creation, update or deletion to kafka connect, and
     * records it in the reconfiguration metrics
     * @param {Function} request async function sending the request
     * @param {Object} [config] configuration sent, if any
     * @returns {Promise<undefined>} undefined
     */
    async _applyChange(request, config = null) {
        const startTime = Date.now();
        try {
            await request();
        } catch (err) {
            this._metricsHandler.onHashedConnectorReconfiguration(false, null, config);
            throw err;
        }
        this._metricsHandler.onHashedConnectorReconfiguration(true, (Date.now() - startTime) / 1000, config);
    }

    /**
     * Deletes connectors
     * @param {string[]} names connector names
     * @returns {Promise<undefined>} undefined
     */
    async _deleteConnectors(names) {
        await eachLimit(names, kafkaConnectConcurrency, async name => {
            await this._applyChange(async () => {
                try {
                    await this._kafkaConnect.deleteConnector(name);
                } catch (err) {
                    if (err.statusCode !== 404) {
                        throw err;
                    }
                }
            });
            this._logger.info('deleted connector', {
                method: 'HashedConnectorsManager._deleteConnectors',
                connector: name,
            });
        });
    }

    /**
     * State of a hashed connector
     * @param {Object} status connector status
     * @returns {string} one of `failed`, `paused` (also when stopped), `running`
     */
    _connectorState(status) {
        if (isConnectorFailed(status)) {
            return 'failed';
        }
        return ['PAUSED', 'STOPPED'].includes(status?.connector?.state) ? 'paused' : 'running';
    }

    /**
     * Lists the managed configuration fields whose live value differs from the
     * spec. Kafka connect stores every value as a string.
     * @param {Object} live live connector configuration
     * @param {Object} spec configuration of the connector spec
     * @returns {string[]} drifted field names
     */
    _configDrift(live, spec) {
        // not compared: the name identifies the connector, it changes with the
        // pipeline (it embeds its hash), and the partition name and start time
        // are set for each connector when it is created
        const unmanagedKeys = ['name', 'pipeline', connectorPartitionNameKey, connectorStartupTimeKey];
        return Object.keys(spec).filter(key =>
            !unmanagedKeys.includes(key) && String(live[key]) !== String(spec[key]));
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
        const partitionName = config[connectorPartitionNameKey];
        // offsets of previous incarnations (other partition names) are stale
        const current = offsets.filter(o => !partitionName || o.partition?.ns === partitionName);
        const committed = minTimestamp(current
            .map(o => timestampFromConnectorOffset(o.offset))
            .filter(ts => ts !== null));
        return committed || timestampFromConnectorStartupTime(config[connectorStartupTimeKey]);
    }

    /**
     * Start oplog time of each missing connector: the earliest time among the
     * outdated connectors covering its collections. A hashed connector with
     * the same number of connectors covers exactly the same collections as
     * the connector spec of the same index, and none of the others'
     * (assuming the way collections are hashed never changes).
     * @param {Object[]} missing missing connector specs, `{ index }`
     * @param {Object[]} outdated outdated connectors with their oplog `time`
     * @returns {Map<number, Object|null>} connector index to `{ t, i }`, null
     * when no outdated connector covers the connector
     */
    _startTimes(missing, outdated) {
        const overlaps = (connector, index) => !connector.parsed ||
            connector.parsed.nbConnectors !== this._nbConnectors || connector.parsed.index === index;
        return new Map(missing.map(({ index }) => [index, minTimestamp(
            outdated.filter(f => overlaps(f, index)).map(f => f.time))]));
    }
}

module.exports = HashedConnectorsManager;
