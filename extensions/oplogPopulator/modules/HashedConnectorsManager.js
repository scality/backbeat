const joi = require('joi');
const async = require('async');
const util = require('util');

const constants = require('../constants');
const KafkaConnectWrapper = require('../../../lib/wrappers/KafkaConnectWrapper');
const OplogPopulatorMetrics = require('../OplogPopulatorMetrics');
const HashedPipelineFactory = require('../pipeline/HashedPipelineFactory');
const { scheduleExclusiveJob } = require('../../../lib/util/scheduleExclusiveJob');
const { buildConnectorConfig, newPartitionName } = require('./connectorConfig');
const ConnectorNameGenerator = require('./ConnectorNameGenerator');
const ListedConnector = require('./ListedConnector');
const { connectorStartupTime, minTimestamp } = require('./resumeToken');

const paramsJoi = joi.object({
    config: joi.object({
        numberOfConnectors: joi.number().integer().min(1).required(),
    }).unknown().required(),
    database: joi.string().required(),
    mongoUrl: joi.string().required(),
    metricsHandler: joi.object().instance(OplogPopulatorMetrics).required(),
    getClusterTime: joi.func().required(),
    logger: joi.object().required(),
}).required();

const eachLimit = util.promisify(async.eachLimit);
const mapLimit = util.promisify(async.mapLimit);
const { kafkaConnectConcurrency } = constants;
// not compared: the name identifies the connector, it changes with the
// pipeline (it embeds its hash), and the partition name and start time
// are set for each connector when it is created
const unmanagedConfigKeys = new Set([
    'name',
    'pipeline',
    'offset.partition.name',
    'startup.mode.timestamp.start.at.operation.time',
]);

// built on the global setTimeout (not timers/promises) so that tests can
// fake it
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * @class HashedConnectorsManager
 *
 * @classdesc Keeps a fixed number N of hashed connectors: connector k
 * ingests the bucket collections whose name hashes to k modulo N.
 */
class HashedConnectorsManager {
    /**
     * @constructor
     * @param {Object} params params
     * @param {Object} params.config oplog populator configuration
     * @param {string} params.database MongoDB database to watch
     * @param {string} params.mongoUrl MongoDB connection url
     * @param {OplogPopulatorMetrics} params.metricsHandler metrics handler
     * @param {Function} params.getClusterTime async function returning the
     * current MongoDB cluster time, `{ t, i }`
     * @param {Logger} params.logger logger
     */
    constructor(params) {
        joi.attempt(params, paramsJoi);
        const { config } = params;
        this._nbConnectors = config.numberOfConnectors;
        this._cronRule = config.connectorsUpdateCronRule;
        this._nameGenerator = new ConnectorNameGenerator(config.prefix);
        this._metricsHandler = params.metricsHandler;
        this._getClusterTime = params.getClusterTime;
        this._logger = params.logger;
        this._kafkaConnect = new KafkaConnectWrapper({
            kafkaConnectHost: config.kafkaConnectHost,
            kafkaConnectPort: config.kafkaConnectPort,
            logger: this._logger,
            metricsHandler: this._metricsHandler,
        });
        const pipelineFactory = new HashedPipelineFactory(config.locationStrippingBytesThreshold);
        this._connectorSpecs = [...Array(this._nbConnectors).keys()].map(index => {
            const pipeline = pipelineFactory.getPipeline({ nbConnectors: this._nbConnectors, index });
            const name = this._nameGenerator.hashedName({ nbConnectors: this._nbConnectors, index, pipeline });
            const connectorConfig = buildConnectorConfig({
                name,
                database: params.database,
                mongoUrl: params.mongoUrl,
                oplogTopic: config.topic,
                heartbeatIntervalMs: config.heartbeatIntervalMs,
            });
            return { index, name, config: { ...connectorConfig, pipeline } };
        });
        this._connectorSpecsByName = new Map(this._connectorSpecs.map(spec => [spec.name, spec]));
    }

    /**
     * Schedules the periodic reconciliation of the connectors
     * @returns {undefined}
     */
    start() {
        this._metricsHandler.onHashedConnectorsStarted(this._connectorSpecs);
        scheduleExclusiveJob(this._cronRule, () => this.reconcile(), this._logger, 'HashedConnectorsManager.reconcile');
    }

    /**
     * Brings the connectors to their specs, in steps each safe to rerun:
     * - repairs the current connectors, first so that a blocked replacement
     *   never delays them
     * - stops the outdated ones, which commits their positions
     * - creates the missing ones from those positions
     * - deletes the outdated ones once every spec exists
     * @returns {Promise<undefined>} undefined
     */
    async reconcile() {
        const { current, outdated, missing } = this._classify(await this._kafkaConnect.getConnectorsWithStatus());
        this._metricsHandler.onConnectorsCounted('outdated', outdated.length);
        await this._repairConnectors(current);
        const notReady = await this._stopConnectors(outdated);
        if (notReady.length > 0) {
            this._logger.warn('outdated connectors not ready to be replaced, retrying later', {
                method: 'HashedConnectorsManager.reconcile',
                connectors: notReady,
            });
            return;
        }
        await this._createConnectors(missing, outdated);
        await this._deleteConnectors(outdated);
    }

    /**
     * Sorts the connectors listed by kafka connect against the connector specs
     * @param {Object} connectors connectors as listed by
     * `KafkaConnectWrapper.getConnectorsWithStatus()`
     * @returns {Object} `{ current, outdated, missing }`: `current` holds
     * `{ connector, spec }`, `outdated` holds `{ connector, parsedName }`
     * (null for a connector that is not hashed), with `ListedConnector`
     * connectors, and `missing` holds connector specs
     */
    _classify(connectors) {
        const plan = { current: [], outdated: [], missing: [] };
        Object.entries(connectors).forEach(([name, { info, status }]) => {
            // a connector whose configuration could not be read is left alone
            if (!this._nameGenerator.isOwned(name) || !info?.config) {
                return;
            }
            const connector = new ListedConnector({ name, config: info.config, status });
            const spec = this._connectorSpecsByName.get(name);
            if (spec) {
                plan.current.push({ connector, spec });
            } else {
                plan.outdated.push({ connector, parsedName: this._nameGenerator.parseHashed(name) });
            }
        });
        plan.missing = this._connectorSpecs.filter(spec => !(spec.name in connectors));
        return plan;
    }

    /**
     * Repairs the current hashed connectors: updates the managed
     * configuration fields that drifted (which restarts its tasks), or else
     * restarts a failed connector; a paused or stopped one is only reported,
     * never resumed
     * @param {Object[]} current current hashed connectors, `{ connector, spec }`
     * @returns {Promise<undefined>} undefined
     */
    async _repairConnectors(current) {
        const counts = { running: 0, paused: 0, failed: 0 };
        await eachLimit(current, kafkaConnectConcurrency, async ({ connector, spec }) => {
            const { name, config, state } = connector;
            counts[state] += 1;
            try {
                const drifted = this._configDrift(config, spec.config);
                if (drifted.length > 0) {
                    const update = { ...config, ...Object.fromEntries(drifted.map(key => [key, spec.config[key]])) };
                    await this._kafkaConnect.updateConnectorConfig(name, update);
                    // values are not logged, the connection uri holds credentials
                    this._logger.info('updated connector configuration', {
                        method: 'HashedConnectorsManager._repairConnectors',
                        connector: name,
                        fields: drifted,
                    });
                } else if (state === 'failed') {
                    connector.failedTasks.forEach(task => this._logger.error('connector task failed', {
                        method: 'HashedConnectorsManager._repairConnectors',
                        connector: name,
                        taskId: task.id,
                        error: task.trace,
                    }));
                    await this._kafkaConnect.restartConnector(name, true, true);
                    this._metricsHandler.onConnectorRestart({ name });
                } else if (state === 'paused') {
                    this._logger.debug('connector is paused or stopped, not resuming it', {
                        method: 'HashedConnectorsManager._repairConnectors',
                        connector: name,
                    });
                }
            } catch (err) {
                this._logger.error('could not update or restart connector', {
                    method: 'HashedConnectorsManager._repairConnectors',
                    connector: name,
                    error: err.description || err.message,
                });
            }
        });
        Object.entries(counts).forEach(([state, count]) => this._metricsHandler.onConnectorsCounted(state, count));
    }

    /**
     * Stops the outdated connectors, which commits their positions; they stay
     * stopped until deleted
     * @param {Object[]} outdated outdated connectors, `{ connector }`
     * @returns {Promise<Object[]>} `{ connector, reason }` for the connectors
     * not ready to be replaced yet, empty if all are
     */
    async _stopConnectors(outdated) {
        if (outdated.length === 0) {
            return [];
        }
        this._logger.info('stopping outdated connectors', {
            method: 'HashedConnectorsManager._stopConnectors',
            connectors: outdated.map(({ connector }) => connector.name),
        });
        const stopRequestedAt = await this._getClusterTime();
        const results = await mapLimit(outdated, kafkaConnectConcurrency,
            async ({ connector }) => this._stopConnector(connector, stopRequestedAt));
        return results.filter(Boolean);
    }

    /**
     * Stops one connector and records its oplog time on it
     * @param {ListedConnector} connector outdated connector
     * @param {Object} stopRequestedAt cluster time `{ t, i }` read before
     * stopping it
     * @returns {Promise<Object|undefined>} `{ connector, reason }` if kafka
     * connect cannot serve offsets yet or its tasks did not stop
     */
    async _stopConnector(connector, stopRequestedAt) {
        const { name } = connector;
        // read first: this tells whether kafka connect serves offsets, and
        // remains the best known position if the connector is deleted meanwhile
        const before = await this._readOffsets(name);
        if (before === undefined) {
            return undefined;
        }
        if (!Array.isArray(before)) {
            return { connector: name, reason: before.retry };
        }
        try {
            await this._kafkaConnect.stopConnector(name);
        } catch (err) {
            if (err.statusCode !== 404) {
                throw err;
            }
            // eslint-disable-next-line no-param-reassign
            connector.oplogTime = connector.oplogTimeFrom(before, stopRequestedAt);
            return undefined;
        }
        if (!await this._waitStopped(name)) {
            return { connector: name, reason: 'connector tasks still running' };
        }
        // stopped tasks commit their final offsets before kafka connect
        // serves this read
        const after = await this._readOffsets(name);
        // deleted meanwhile: the first read is the best known position
        const offsets = Array.isArray(after) ? after : before;
        // eslint-disable-next-line no-param-reassign
        connector.oplogTime = connector.oplogTimeFrom(offsets, stopRequestedAt);
        return undefined;
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
        const deadline = Date.now() + constants.connectorStopGracePeriodMs;
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
            await sleep(constants.connectorStopPollIntervalMs);
        }
        return false;
    }

    /**
     * Creates the missing hashed connectors, from the oplog time reached by
     * the outdated connectors covering their collections, or else from the
     * current cluster time
     * @param {Object[]} missing specs of the connectors to create
     * @param {Object[]} outdated stopped outdated connectors, `{ connector, parsedName }`
     * @returns {Promise<undefined>} undefined
     */
    async _createConnectors(missing, outdated) {
        if (missing.length === 0) {
            return;
        }
        const now = await this._getClusterTime();
        await eachLimit(missing, kafkaConnectConcurrency, async ({ index, name, config }) => {
            const start = this._startTime(index, outdated) || now;
            const connectorConfig = {
                ...config,
                'offset.partition.name': newPartitionName(),
                'startup.mode.timestamp.start.at.operation.time': connectorStartupTime(start),
            };
            // a 409 also answers requests during a worker rebalance: failing
            // here keeps the outdated connectors until the next reconciliation
            await this._kafkaConnect.createConnector({ name, config: connectorConfig });
            this._logger.info('created connector', {
                method: 'HashedConnectorsManager._createConnectors',
                connector: name,
                start,
            });
        });
    }

    /**
     * Deletes the outdated connectors
     * @param {Object[]} outdated outdated connectors, `{ connector }`
     * @returns {Promise<undefined>} undefined
     */
    async _deleteConnectors(outdated) {
        await eachLimit(outdated, kafkaConnectConcurrency, async ({ connector }) => {
            try {
                await this._kafkaConnect.deleteConnector(connector.name);
            } catch (err) {
                if (err.statusCode !== 404) {
                    throw err;
                }
            }
            this._logger.info('deleted connector', {
                method: 'HashedConnectorsManager._deleteConnectors',
                connector: connector.name,
            });
        });
    }

    /**
     * Lists the managed configuration fields whose live value differs from the
     * spec. Kafka connect stores every value as a string.
     * @param {Object} live live connector configuration
     * @param {Object} spec configuration of the connector spec
     * @returns {string[]} drifted field names
     */
    _configDrift(live, spec) {
        return Object.keys(spec).filter(key =>
            !unmanagedConfigKeys.has(key) && String(live[key]) !== String(spec[key]));
    }

    /**
     * Start oplog time of a missing connector: the earliest oplog time of
     * the outdated connectors covering its collections, i.e. all of them
     * except hashed connectors with the same N and another index
     * @param {number} index index of the missing connector
     * @param {Object[]} outdated stopped outdated connectors, `{ connector, parsedName }`
     * @returns {Object|null} `{ t, i }`, or null if none is known
     */
    _startTime(index, outdated) {
        const covering = outdated.filter(({ connector, parsedName }) => connector.oplogTime && (!parsedName ||
            parsedName.nbConnectors !== this._nbConnectors || parsedName.index === index));
        return minTimestamp(covering.map(({ connector }) => connector.oplogTime));
    }
}

module.exports = HashedConnectorsManager;
