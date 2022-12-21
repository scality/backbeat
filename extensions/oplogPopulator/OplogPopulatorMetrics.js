const { ZenkoMetrics } = require('arsenal').metrics;

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
            name: 'oplog_populator_acknowledgement_lag_sec',
            help: 'Delay between a config change in mongo and the start of processing by the oplogPopulator in seconds',
            labelNames: ['opType'],
            buckets: [0.001, 0.01, 1, 10, 100, 1000, 10000],
        });
        this.connectorConfiguration = ZenkoMetrics.createCounter({
            name: 'oplog_populator_connector_configuration',
            help: 'Number of times we update the configuration of a connector',
            labelNames: ['connector', 'opType'],
        });
        this.buckets = ZenkoMetrics.createGauge({
            name: 'oplog_populator_connector_buckets',
            help: 'Number of buckets per connector',
            labelNames: ['connector'],
        });
        this.requestSize = ZenkoMetrics.createCounter({
            name: 'oplog_populator_connector_request_bytes',
            help: 'Size of kafka connect request in bytes',
            labelNames: ['connector'],
        });
        this.mongoPipelineSize = ZenkoMetrics.createGauge({
            name: 'oplog_populator_connector_pipeline_bytes',
            help: 'Size of mongo pipeline in bytes',
            labelNames: ['connector'],
        });
        this.connectors = ZenkoMetrics.createGauge({
            name: 'oplog_populator_connectors',
            help: 'Total number of configured connectors',
            labelNames: ['isOld'],
        });
        this.reconfigurationLag = ZenkoMetrics.createHistogram({
            name: 'oplog_populator_reconfiguration_lag_sec',
            help: 'Time it takes kafka-connect to respond to a connector configuration request',
            labelNames: ['connector'],
            buckets: [0.001, 0.01, 1, 10, 100, 1000, 10000],
        });
        this.connectorConfigurationApplied = ZenkoMetrics.createCounter({
            name: 'oplog_populator_connector_configuration_applied',
            help: 'Number of times we submit the configuration of a connector to kafka-connect',
            labelNames: ['connector', 'success'],
        });
    }

    /**
     * updates oplog_populator_acknowledgement_lag_sec metric
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
     * updates oplog_populator_connector_configuration &
     * oplog_populator_connector_request_bytes metrics
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
     * updates oplog_populator_connectors metric
     * @param {boolean} isOld true if connectors were not
     * created by this OplogPopulator instance
     * @param {number} count number of connectors added
     * @returns {undefined}
     */
    onConnectorsInstantiated(isOld, count = 1) {
        try {
            this.connectors.inc({
                isOld,
            }, count);
        } catch (error) {
            this._logger.error('An error occured while pushing metrics', {
                method: 'OplogPopulatorMetrics.onConnectorsInstantiated',
                error: error.message,
            });
        }
    }

    /**
     * updates oplog_populator_reconfiguration_lag_sec metric
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
}

module.exports = OplogPopulatorMetrics;
