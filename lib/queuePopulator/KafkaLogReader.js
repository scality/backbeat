const LogConsumer = require('./KafkaLogConsumer/LogConsumer');
const LogReader = require('./LogReader');

class KafkaLogReader extends LogReader {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.zkClient - zookeeper client object
     * @param {Object} params.zkConfig - zookeeper config
     * @param {Object} params.kafkaConfig - kafka configuration object
     * @param {Logger} params.logger - logger object
     * @param {Object} params.qpKafkaConfig - queue populator kafka configuration
     * @param {QueuePopulatorExtension[]} params.extensions - array of
     *   queue populator extension modules
     * @param {MetricsProducer} params.metricsProducer - instance of metrics
     *   producer
     * @param {MetricsHandler} params.metricsHandler - instance of metrics
     *   handler
     */
    constructor(params) {
        const { zkClient, kafkaConfig, zkConfig, qpKafkaConfig,
            logger, extensions, metricsProducer, metricsHandler } = params;
        // conf contains global kafka and queuePoplator kafka configs
        const conf = {
            hosts: kafkaConfig.hosts,
            ...qpKafkaConfig
        };
        logger.info('initializing kafka log reader',
            { method: 'KafkaLogReader.constructor',
            kafkaConfig: conf });
        const logConsumer = new LogConsumer(conf, logger);
        super({ zkClient, kafkaConfig, zkConfig, logConsumer,
                logId: `kafka_${qpKafkaConfig.logName}`, logger, extensions,
                metricsProducer, metricsHandler });
        this._kafkaConfig = conf;
    }

    /**
    * Setup log consumer
    * @param {object} done callback function
    * @return {undefined}
    */
     setup(done) {
        this.logConsumer.setup(err => {
            if (err) {
                this.log.error('error setting up log consumer', {
                    method: 'KafkaLogReader.setup',
                    kafkaConfig: this._kafkaConfig,
                });
                return done(err);
            }
            return super.setup(done);
        });
    }

    /**
     * Get kakfa log info
     * @return {object} logName
     */
    getLogInfo() {
        return { logName: this._kafkaConfig.logName };
    }

    /**
     * Get metric label
     * @returns {object} metric labels
     */
    getMetricLabels() {
        return {
            logName: 'kafka-log',
            logId: this._kafkaConfig.logName,
        };
    }
}

module.exports = KafkaLogReader;
