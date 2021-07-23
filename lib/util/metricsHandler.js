const promClient = require('prom-client');

const { wrapCounterInc, wrapGaugeSet, wrapHistogramObserve } = require('./metrics');

promClient.register.setDefaultLabels({
    origin: 'replication',
    containerName: process.env.CONTAINER_NAME || '',
});

/**
 * Labels used for Prometheus metrics
 * @typedef {Object} MetricLabels
 * @property {string} origin - Method that began the replication
 * @property {string} logName - Name of the log we are using
 * @property {string} logId - Id of the log we are reading
 * @property {string} containerName - Name of the container running our process
 * @property {string} [publishStatus] - Result of the publishing to kafka to the topic
 * @property {string} [replicationStatus] - Result of the replications status
 * @property {string} [partition] - What kafka partition relates to the metric
 * @property {string} [serviceName] - Name of our service to match generic metrics
 * @property {string} [replicationContent] - Data or Metadata
 * @property {string} [replicationStage] - Name of the replication stage
 */

const logReadOffsetMetric = new promClient.Gauge({
    name: 'replication_read_offset',
    help: 'Current read offset of metadata journal',
    labelNames: ['origin', 'logName', 'logId', 'containerName'],
});

const logSizeMetric = new promClient.Gauge({
    name: 'replication_log_size',
    help: 'Current size of metadata journal',
    labelNames: ['origin', 'logName', 'logId', 'containerName'],
});

const messageMetrics = new promClient.Counter({
    name: 'replication_populator_messages',
    help: 'Total number of Kafka messages produced by the queue populator',
    labelNames: ['origin', 'logName', 'logId', 'containerName', 'publishStatus'],
});

const objectMetrics = new promClient.Counter({
    name: 'replication_populator_objects',
    help: 'Total objects queued for replication',
    labelNames: ['origin', 'logName', 'logId', 'containerName'],
});

const byteMetrics = new promClient.Counter({
    name: 'replication_populator_bytes',
    help: 'Total number of bytes queued for replication not including metadata',
    labelNames: ['origin', 'logName', 'logId', 'containerName'],
});

const replicationStatusMetric = new promClient.Counter({
    name: 'replication_status_changed_total',
    help: 'Number of objects updated',
    labelNames: ['origin', 'containerName', 'replicationStatus'],
});

const kafkaLagMetric = new promClient.Gauge({
    name: 'kafka_lag',
    help: 'Number of update entries waiting to be consumed from the Kafka topic',
    labelNames: ['origin', 'containerName', 'partition', 'serviceName'],
});

const dataReplicationStatusMetric = new promClient.Counter({
    name: 'replication_data_status_changed_total',
    help: 'Number of status updates for data operation',
    labelNames: ['origin', 'containerName', 'replicationStatus'],
});

const metadataReplicationStatusMetric = new promClient.Counter({
    name: 'replication_metadata_status_changed_total',
    help: 'Number of status updates for metadata operation',
    labelNames: ['origin', 'containerName', 'replicationStatus'],
});

const dataReplicationBytesMetric = new promClient.Counter({
    name: 'replication_data_bytes',
    help: 'Total number of bytes replicated for data operation',
    labelNames: ['origin', 'containerName', 'serviceName'],
});

const metadataReplicationBytesMetric = new promClient.Counter({
    name: 'replication_metadata_bytes',
    help: 'Total number of bytes replicated for metadata operation',
    labelNames: ['origin', 'containerName', 'serviceName'],
});

const sourceDataBytesMetric = new promClient.Counter({
    name: 'replication_source_data_bytes',
    help: 'Total number of data bytes read from replication source',
    labelNames: ['origin', 'containerName', 'serviceName'],
});

const readMetric = new promClient.Counter({
    name: 'replication_data_read',
    help: 'Number of read operations',
    labelNames: ['origin', 'containerName', 'serviceName'],
});

const writeMetric = new promClient.Counter({
    name: 'replication_data_write',
    help: 'Number of write operations',
    labelNames: ['origin', 'containerName', 'serviceName', 'replicationContent'],
});

const timeElapsedMetric = new promClient.Histogram({
    name: 'replication_stage_time_elapsed',
    help: 'Elapsed time of a specific stage in replication',
    labelNames: ['origin', 'containerName', 'serviceName', 'replicationStage'],
});

/**
 * Contains methods to incrememt different metrics
 * @typedef {Object} MetricsHandler
 * @property {CounterInc} messages - Increments the message metric
 * @property {CounterInc} objects - Increments the objects metric
 * @property {CounterInc} bytes - Increments the bytes metric
 * @property {GaugeSet} logReadOffset - Set the log read offset metric
 * @property {GaugeSet} logSize - Set the log size metric
 * @property {CounterInc} status - Increments the replication status metric
 * @property {CounterInc} dataReplicationStatus - Increments the replication status metric for data operation
 * @property {CounterInc} metadataReplicationStatus - Increments the replication status metric for metadata operation
 * @property {CounterInc} dataReplicationBytes - Increments the replication bytes metric for data operation
 * @property {CounterInc} metadataReplicationBytes - Increments the replication bytes metric for metadata operation
 * @property {CounterInc} sourceDataBytes - Increments the source data bytes metric
 * @property {GaugeSet} lag - Set the kafka lag metric
 * @property {CounterInc} reads - Increments the read metric
 * @property {CounterInc} writes - Increments the write metric
 * @property {HistogramObserve} timeElapsed - Observes the time elapsed metric
 */
const metricsHandler = {
    messages: wrapCounterInc(messageMetrics),
    objects: wrapCounterInc(objectMetrics),
    bytes: wrapCounterInc(byteMetrics),
    logReadOffset: wrapGaugeSet(logReadOffsetMetric),
    logSize: wrapGaugeSet(logSizeMetric),
    status: wrapCounterInc(replicationStatusMetric),
    dataReplicationStatus: wrapCounterInc(dataReplicationStatusMetric),
    metadataReplicationStatus: wrapCounterInc(metadataReplicationStatusMetric),
    dataReplicationBytes: wrapCounterInc(dataReplicationBytesMetric),
    metadataReplicationBytes: wrapCounterInc(metadataReplicationBytesMetric),
    sourceDataBytes: wrapCounterInc(sourceDataBytesMetric),
    lag: wrapGaugeSet(kafkaLagMetric),
    reads: wrapCounterInc(readMetric),
    writes: wrapCounterInc(writeMetric),
    timeElapsed: wrapHistogramObserve(timeElapsedMetric),
};

module.exports = metricsHandler;
