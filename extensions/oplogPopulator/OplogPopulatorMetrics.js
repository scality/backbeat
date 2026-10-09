const { ZenkoMetrics } = require('@scality/arsenal').metrics;

const { getStringSizeInBytes } = require('../../lib/util/buffer');

class OplogPopulatorMetrics {
    /**
     * @param {Logger} logger logger instance
     */
    constructor(logger) {
        this.acknowledgementLag = null;
        this.connectorConfiguration = null;
        this.requestSize = null;
        this.connectors = null;
        this.reconfigurationLag = null;
        this.connectorConfigurationApplied = null;
        this._logger = logger;
    }

    registerMetrics() {
        this.acknowledgementLag = ZenkoMetrics.createHistogram({
            name: 's3_oplog_populator_acknowledgement_lag_seconds',
            help: 'Delay between a config change in mongo and the start of processing by the oplogPopulator in seconds',
            labelNames: ['opType'],
            buckets: [0.001, 0.01, 1, 10, 100, 1000, 10000],
        });
        this.connectorConfiguration = ZenkoMetrics.createCounter({
            name: 's3_oplog_populator_connector_configurations_total',
            help: 'Total number of connector configuration updates',
            labelNames: ['connector', 'opType'],
        });
        this.buckets = ZenkoMetrics.createGauge({
            name: 's3_oplog_populator_connector_buckets',
            help: 'Total number of buckets per connector',
            labelNames: ['connector'],
        });
        this.bucketsExceedingLimit = ZenkoMetrics.createGauge({
            name: 's3_oplog_populator_buckets_exceeding_limit',
            help: 'Total number of buckets exceeding the limit for all connectors',
        });
        this.retainedBuckets = ZenkoMetrics.createGauge({
            name: 's3_oplog_populator_retained_buckets',
            help: 'Current number of buckets still listened to by immutable connectors despite intended removal',
        });
        this.requestSize = ZenkoMetrics.createCounter({
            name: 's3_oplog_populator_connector_request_bytes_total',
            help: 'Total size of kafka connect request in bytes',
            labelNames: ['connector'],
        });
        this.mongoPipelineSize = ZenkoMetrics.createGauge({
            name: 's3_oplog_populator_connector_pipeline_bytes',
            help: 'Size of mongo pipeline in bytes',
            labelNames: ['connector'],
        });
        this.connectors = ZenkoMetrics.createGauge({
            name: 's3_oplog_populator_connectors',
            help: 'Total number of configured connectors',
        });
        this.reconfigurationLag = ZenkoMetrics.createHistogram({
            name: 's3_oplog_populator_reconfiguration_lag_seconds',
            help: 'Time it takes kafka-connect to respond to a connector configuration request',
            labelNames: ['connector'],
            buckets: [0.001, 0.01, 1, 10, 100, 1000, 10000],
        });
        this.connectorConfigurationApplied = ZenkoMetrics.createCounter({
            name: 's3_oplog_populator_connector_configuration_applied_total',
            help: 'Total number of connector configuration submissions to kafka-connect',
            labelNames: ['connector', 'success'],
        });
        this.connectorRestarts = ZenkoMetrics.createCounter({
            name: 's3_oplog_populator_connector_restarts',
            help: 'Total number of connector restarts',
            labelNames: ['connector'],
        });
        this.hashedConnectors = ZenkoMetrics.createGauge({
            name: 's3_oplog_populator_hashed_connectors',
            help: 'Number of hashed connectors, by state',
            labelNames: ['state'],
        });
        this.newerGenerationConnectors = ZenkoMetrics.createGauge({
            name: 's3_oplog_populator_newer_generation_connectors',
            help: 'Number of connectors created by a newer oplog populator, which this one leaves alone',
        });
        this.outdatedConnectors = ZenkoMetrics.createGauge({
            name: 's3_oplog_populator_outdated_connectors',
            help: 'Number of connectors to migrate to hashed connectors',
        });
        this.migrations = ZenkoMetrics.createCounter({
            name: 's3_oplog_populator_migrations_total',
            help: 'Total number of migrations to hashed connectors',
            labelNames: ['success'],
        });
        this.migrationInProgress = ZenkoMetrics.createGauge({
            name: 's3_oplog_populator_migration_in_progress',
            help: 'Whether a migration to hashed connectors is in progress',
        });
        this.migrationStartTimeAge = ZenkoMetrics.createGauge({
            name: 's3_oplog_populator_migration_start_time_age_seconds',
            help: 'Age of the oplog time the last migration restarted from',
        });
    }

    /**
     * updates s3_oplog_populator_acknowledgement_lag_seconds metric
     * @param {string} opType oplog operation type
     * @param {number} delta delay between a config change
     * in mongo and it getting processed by the oplogPopulator
     * @returns {undefined}
     */
    onOplogEventProcessed(opType, delta) {
        try {
            this.acknowledgementLag.observe({
                opType,
            }, delta);
        } catch (error) {
            this._logger.error('An error occured while pushing metric', {
                method: 'OplogPopulatorMetrics.onOplogEventProcessed',
                error: error.message,
            });
        }
    }

    /**
     * updates s3_oplog_populator_connector_configurations_total &
     * s3_oplog_populator_connector_request_bytes_total metrics
     * @param {Connector} connector connector instance
     * @param {string} opType operation type, could be one of
     * "add" and "delete"
     * @param {number} buckets number of buckets updated
     * @returns {undefined}
     */
    onConnectorConfigured(connector, opType, buckets = 1) {
        try {
            this.connectorConfiguration.inc({
                connector: connector.name,
                opType,
            }, buckets);
            const reqSize = getStringSizeInBytes(JSON.stringify(connector.config));
            this.requestSize.inc({
                connector: connector.name,
            }, reqSize);
            const pipelineSize = getStringSizeInBytes(JSON.stringify(connector.config.pipeline));
            this.mongoPipelineSize.set({
                connector: connector.name,
            }, pipelineSize);
        } catch (error) {
            this._logger.error('An error occured while pushing metrics', {
                method: 'OplogPopulatorMetrics.onConnectorConfigured',
                error: error.message,
            });
        }
    }

    /**
     * updates s3_oplog_populator_connectors metric
     * @param {boolean} isOld true if connectors were not
     * created by this OplogPopulator instance
     * @param {number} count number of connectors added
     * @returns {undefined}
     */
    onConnectorsInstantiated(isOld, count = 1) {
        try {
            this.connectors.inc(count);
        } catch (error) {
            this._logger.error('An error occured while pushing metrics', {
                method: 'OplogPopulatorMetrics.onConnectorsInstantiated',
                error: error.message,
            });
        }
    }

    /**
     * updates s3_oplog_populator_connectors metric
     * when a connector is destroyed
     * @returns {undefined}
     */
    onConnectorDestroyed() {
        try {
            this.connectors.dec(1);
        } catch (error) {
            this._logger.error('An error occurred while pushing metrics', {
                method: 'OplogPopulatorMetrics.onConnectorDestroyed',
                error: error.message,
            });
        }
    }

    /**
     * updates s3_oplog_populator_reconfiguration_lag_seconds metric
     * @param {Connector} connector connector instance
     * @param {Boolean} success true if reconfiguration was successful
     * @param {number} delta time it takes to reconfigure a connector
     * @returns {undefined}
     */
    onConnectorReconfiguration(connector, success, delta = null) {
        try {
            this.connectorConfigurationApplied.inc({
                connector: connector.name,
                success,
            });
            if (success) {
                this.reconfigurationLag.observe({
                    connector: connector.name,
                }, delta);
                this.buckets.set({
                    connector: connector.name,
                }, connector.bucketCount);
            }
        } catch (error) {
            this._logger.error('An error occured while pushing metrics', {
                method: 'OplogPopulatorMetrics.onConnectorReconfiguration',
                error: error.message,
            });
        }
    }

    /**
     * updates s3_oplog_populator_connector_restarts metric
     * @param {string} connector connector name
     * @returns {undefined}
     */
    onConnectorRestart(connector) {
        try {
            this.connectorRestarts.inc({
                connector: connector.name,
            });
        } catch (error) {
            this._logger.error('An error occured while pushing metric', {
                method: 'OplogPopulatorMetrics.onConnectorRestarted',
                error: error.message,
            });
        }
    }

    /**
     * updates metrics when the connectors are reconciled
     * @param {number} bucketsExceedingLimit number of buckets above the limit
     * for all connectors
     * @param {number} retainedBuckets number of buckets still listened to by
     * immutable connectors despite intended removal
     * @returns {undefined}
     */
    onConnectorsReconciled(bucketsExceedingLimit, retainedBuckets) {
        try {
            this.bucketsExceedingLimit.set(bucketsExceedingLimit);
            this.retainedBuckets.set(retainedBuckets);
        } catch (error) {
            this._logger.error('An error occured while pushing metric', {
                method: 'OplogPopulatorMetrics.onConnectorsReconciled',
                error: error.message,
            });
        }
    }

    /**
     * updates the gauges describing the connectors seen by the hashed mode
     * @param {Object} counts counts
     * @param {number} counts.connectors number of managed connectors
     * @param {Object} counts.states number of hashed connectors per state
     * @param {number} counts.outdated number of connectors to migrate
     * @param {number} counts.newer number of newer generation connectors
     * @returns {undefined}
     */
    onHashedConnectorsObserved({ connectors, states, outdated, newer }) {
        try {
            this.connectors.set(connectors);
            Object.entries(states).forEach(([state, count]) => this.hashedConnectors.set({ state }, count));
            this.outdatedConnectors.set(outdated);
            this.newerGenerationConnectors.set(newer);
        } catch (error) {
            this._logger.error('An error occured while pushing metrics', {
                method: 'OplogPopulatorMetrics.onHashedConnectorsObserved',
                error: error.message,
            });
        }
    }

    /**
     * updates s3_oplog_populator_migration_in_progress metric
     * @param {boolean} inProgress whether a migration is in progress
     * @returns {undefined}
     */
    onMigrationInProgress(inProgress) {
        try {
            this.migrationInProgress.set(inProgress ? 1 : 0);
        } catch (error) {
            this._logger.error('An error occured while pushing metric', {
                method: 'OplogPopulatorMetrics.onMigrationInProgress',
                error: error.message,
            });
        }
    }

    /**
     * updates the metrics of a completed migration attempt
     * @param {boolean} success whether the migration completed
     * @param {number|null} startTimeAge age of the oplog time the hashed
     * connectors restarted from, in seconds
     * @returns {undefined}
     */
    onMigration(success, startTimeAge = null) {
        try {
            this.migrations.inc({ success });
            if (startTimeAge !== null) {
                this.migrationStartTimeAge.set(startTimeAge);
            }
        } catch (error) {
            this._logger.error('An error occured while pushing metrics', {
                method: 'OplogPopulatorMetrics.onMigration',
                error: error.message,
            });
        }
    }
}

module.exports = OplogPopulatorMetrics;
