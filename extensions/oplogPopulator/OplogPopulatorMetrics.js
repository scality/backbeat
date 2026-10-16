const { ZenkoMetrics } = require('@scality/arsenal').metrics;

const { getStringSizeInBytes } = require('../../lib/util/buffer');

// connector label of the reconfiguration metrics of all hashed connectors
const hashedConnectorsLabel = 'hashed';

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
            help: 'Total number of configured connectors, by state with hashed ingestion',
            labelNames: ['state'],
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
        this.migrations = ZenkoMetrics.createCounter({
            name: 's3_oplog_populator_migrations_total',
            help: 'Total number of migrations to hashed connectors',
            labelNames: ['success'],
        });
        // exposed from the start, so that the first migration shows as an increase
        [true, false].forEach(success => this.migrations.inc({ success }, 0));
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
     * updates s3_oplog_populator_connectors metric with the connectors seen by
     * the hashed mode
     * @param {Object} states number of connectors per state: running, paused
     * and failed hashed connectors, and outdated connectors to migrate
     * @returns {undefined}
     */
    onHashedConnectorsObserved(states) {
        try {
            Object.entries(states).forEach(([state, count]) => this.connectors.set({ state }, count));
        } catch (error) {
            this._logger.error('An error occured while pushing metrics', {
                method: 'OplogPopulatorMetrics.onHashedConnectorsObserved',
                error: error.message,
            });
        }
    }

    /**
     * updates s3_oplog_populator_connector_pipeline_bytes metric of a hashed
     * connector
     * @param {string} connector connector name
     * @param {string} pipeline connector pipeline
     * @returns {undefined}
     */
    onHashedConnectorPipeline(connector, pipeline) {
        try {
            this.mongoPipelineSize.set({ connector }, getStringSizeInBytes(JSON.stringify(pipeline)));
        } catch (error) {
            this._logger.error('An error occured while pushing metrics', {
                method: 'OplogPopulatorMetrics.onHashedConnectorPipeline',
                error: error.message,
            });
        }
    }

    /**
     * creates the reconfiguration metrics of hashed connectors at 0, so that
     * their first increase shows: each connector is only created or deleted
     * once, so they are recorded under a single label for all of them
     * @returns {undefined}
     */
    onHashedConnectorsStarted() {
        try {
            const connector = hashedConnectorsLabel;
            [true, false].forEach(success => this.connectorConfigurationApplied.inc({ connector, success }, 0));
            this.requestSize.inc({ connector }, 0);
            this.reconfigurationLag.zero({ connector });
        } catch (error) {
            this._logger.error('An error occured while pushing metrics', {
                method: 'OplogPopulatorMetrics.onHashedConnectorsStarted',
                error: error.message,
            });
        }
    }

    /**
     * updates the metrics of a hashed connector creation, update or deletion
     * @param {boolean} success whether kafka connect applied it
     * @param {number} delta duration of the request, in seconds
     * @param {Object} [config] configuration sent, if any
     * @returns {undefined}
     */
    onHashedConnectorReconfiguration(success, delta, config = null) {
        try {
            const connector = hashedConnectorsLabel;
            this.connectorConfigurationApplied.inc({ connector, success });
            if (success) {
                this.reconfigurationLag.observe({ connector }, delta);
            }
            if (config) {
                this.requestSize.inc({ connector }, getStringSizeInBytes(JSON.stringify(config)));
            }
        } catch (error) {
            this._logger.error('An error occured while pushing metrics', {
                method: 'OplogPopulatorMetrics.onHashedConnectorReconfiguration',
                error: error.message,
            });
        }
    }

    /**
     * updates s3_oplog_populator_migrations_total metric
     * @param {boolean} success whether the migration completed
     * @returns {undefined}
     */
    onMigration(success) {
        try {
            this.migrations.inc({ success });
        } catch (error) {
            this._logger.error('An error occured while pushing metrics', {
                method: 'OplogPopulatorMetrics.onMigration',
                error: error.message,
            });
        }
    }
}

module.exports = OplogPopulatorMetrics;
