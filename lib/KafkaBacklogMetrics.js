const async = require('async');
const { EventEmitter } = require('events');
const zookeeper = require('node-zookeeper-client');

const Logger = require('werelogs').Logger;
const { errors, metrics } = require('arsenal');

const zookeeperHelper = require('./clients/zookeeper');
const { readUInt64BE } = require('./util/buffer');
const { promMetricNames } = require('./constants').kafkaBacklogMetrics;

const latestPublishedMessageTimestampGauge = metrics.ZenkoMetrics.createGauge({
    name: promMetricNames.latestPublishedMessageTimestamp,
    help: 'Timestamp of latest published message',
    labelNames: ['topic', 'partition'],
});

const deliveryReportsCounter = metrics.ZenkoMetrics.createCounter({
    name: promMetricNames.deliveryReportsTotal,
    help: 'Number of delivery reports received',
    labelNames: ['status'],
});

const latestConsumedMessageTimestampGauge = metrics.ZenkoMetrics.createGauge({
    name: promMetricNames.latestConsumedMessageTimestamp,
    help: 'Timestamp of latest consumed message',
    // "consumergroup" is lowercase to match convention of metrics
    // exposed by Kafka.
    labelNames: ['topic', 'partition', 'consumergroup'],
});

const latestConsumeEventTimestampGauge = metrics.ZenkoMetrics.createGauge({
    name: promMetricNames.latestConsumeEventTimestamp,
    help: 'Timestamp of last time a consumer consumed a message',
    // "consumergroup" is lowercase to match convention of metrics
    // exposed by Kafka.
    labelNames: ['topic', 'partition', 'consumergroup'],
});

// global error instances for private use
const CheckConditionError = new Error();
const NoNodeError = new Error();

// special value for the returned offset when the node holding it is outdated
const OutdatedOffset = {};

class KafkaBacklogMetrics extends EventEmitter {
    constructor(zookeeperEndpoint, backlogMetricsConfig) {
        super();
        this._zookeeperEndpoint = zookeeperEndpoint;
        this._zookeeperReady = false;
        this._backlogMetricsConfig = backlogMetricsConfig;
        this._log = new Logger('KafkaBacklogMetrics');
    }

    init() {
        this._initZookeeperClient();
    }

    _initZookeeperClient() {
        this._zookeeper = zookeeperHelper.createClient(this._zookeeperEndpoint);
        this._zookeeper.connect();
        this._zookeeper.on('error', err => {
            this.emit('error', err);
        });
        this._zookeeper.once('ready', () => {
            this.emit('ready');
        });
    }

    isReady() {
        if (!(this._zookeeper &&
            this._zookeeper.getState().code ===
            zookeeper.State.SYNC_CONNECTED.code)) {
            this._log.error('KafkaBacklogMetrics is not ready, zookeeper not connected',
                { error: this._zookeeper.getState().code });
            return false;
        }
        return true;
    }

    /**
     * This function updates the Zenko metrics with the latest
     * published message timestamp (metrics are published normally via
     * a Prometheus endpoint in the process).
     *
     * @param {string} topic - topic name
     * @param {number} partition - partition number of published message
     * @param {number} timestamp - timestamp as seconds since epoch
     * @return {undefined}
     */
    static onMessagePublished(topic, partition, timestamp) {
        latestPublishedMessageTimestampGauge.set({
            topic, partition,
        }, timestamp);
    }

    /**
     * This function tracks delivery reports received metrics
     *
     * @param {Error} [error] - Error received in the delivery report
     * callback, if any
     * @return {undefined}
     */
    static onDeliveryReportReceived(error) {
        deliveryReportsCounter.inc({
            status: error ? 'error' : 'success',
        });
    }

    /**
     * This function updates the Zenko metrics with the latest
     * consumed message timestamp, and the consumption event timestamp
     * (metrics are published normally via a Prometheus endpoint in
     * the process).
     *
     * @param {string} topic - topic name
     * @param {number} partition - partition number of consumed message
     * @param {string} consumerGroup - name of consumer group
     * @param {number} timestamp - timestamp as seconds since epoch
     * @return {undefined}
     */
    static onMessageConsumed(topic, partition, consumerGroup, timestamp) {
        latestConsumedMessageTimestampGauge.set({
            topic, partition, consumergroup: consumerGroup,
        }, timestamp);

        latestConsumeEventTimestampGauge.set({
            topic, partition, consumergroup: consumerGroup,
        }, Date.now() / 1000);
    }

    _getPartitionsOffsetsZkPath(topic) {
        return `${this._backlogMetricsConfig.zkPath}/${topic}`;
    }

    _getOffsetZkPath(topic, partition, offsetType, label) {
        const basePath = `${this._getPartitionsOffsetsZkPath(topic)}/` +
              `${partition}/${offsetType}`;
        if (label) {
            return `${basePath}/${label}`;
        }
        return basePath;
    }

    _publishOffset(topic, partition, offset, offsetType, label, cb) {
        const zkPath = this._getOffsetZkPath(
            topic, partition, offsetType, label);
        const zkData = Buffer.from(offset.toString());
        this._log.debug('publishing kafka offset to zookeeper', {
            topic, partition, offset, offsetType, label,
        });
        this._zookeeper.setOrCreate(zkPath, zkData, err => {
            if (err) {
                this._log.error('error publishing offset to zookeeper', {
                    zkPath, topic, partition, offset, offsetType, label,
                    error: err.message,
                });
                return cb(err);
            }
            this._log.debug('published offset to zookeeper', {
                zkPath, topic, partition, offset, offsetType, label,
            });
            return cb();
        });
    }

    _readOffset(topic, partition, offsetType, label, cb) {
        const zkPath = this._getOffsetZkPath(
            topic, partition, offsetType, label);
        this._log.debug('reading kafka offset from zookeeper', {
            topic, partition, offsetType, label,
        });
        this._zookeeper.getData(zkPath, (err, offsetData, offsetStat) => {
            if (err) {
                if (err.getCode() === zookeeper.Exception.NO_NODE) {
                    this._log.debug(
                        'requested kafka offset node does not exist', {
                            topic, partition, offsetType, label,
                        });
                    return cb(NoNodeError);
                }
                this._log.error(
                    'error reading kafka offset from zookeeper', {
                        topic, partition, offsetType, label,
                        error: err.message,
                    });
                return cb(errors.InternalError);
            }
            const mtime = readUInt64BE(offsetStat.mtime);
            const lastModAgeMs = Date.now() - mtime;
            // if mtime of a consumer or topic node is older than
            // twice the regular update interval, we consider it's an
            // outdated node, ignore and delete it. This may be due to
            // a deleted location, or partitions with no active
            // consumer yet. Snapshot nodes are allowed to be older
            // since they are not regularly refreshed, but then the
            // topic offset might still be detected as outdated.
            if ((offsetType === 'consumers' || offsetType === 'topic') &&
                lastModAgeMs >
                this._backlogMetricsConfig.intervalS * 1000 * 2) {
                this._log.debug(
                    'requested kafka offset mtime is too old, deleting it', {
                        topic, partition, offsetType, label, lastModAgeMs,
                    });
                return this._zookeeper.remove(zkPath, -1, err => {
                    if (err) {
                        this._log.warn(
                            'error deleting outdated kafka offset node ' +
                                'from zookeeper', {
                                    topic, partition, offsetType, label,
                                    zkPath,
                                    error: err.message,
                                });
                    }
                    // return OutdatedOffset object for an outdated
                    // offset, later we will detect this special value
                    // and ignore it
                    return cb(null, OutdatedOffset);
                });
            }
            let offset;
            try {
                offset = JSON.parse(offsetData);
            } catch (err) {
                this._log.error('malformed JSON data for offset', {
                    topic, partition, offsetType, label,
                    error: err.message,
                });
                return cb(errors.InternalError);
            }
            if (!Number.isInteger(offset)) {
                this._log.error('offset not a number', {
                    topic, partition, offsetType, label,
                });
                return cb(errors.InternalError);
            }
            return cb(null, offset);
        });
    }

    _readAllOffsets(topic, partition, offsetType, cb) {
        const zkPath = this._getOffsetZkPath(topic, partition, offsetType);
        this._zookeeper.getChildren(zkPath, (err, labels) => {
            if (err) {
                if (err.getCode() === zookeeper.Exception.NO_NODE) {
                    // no label has been published yet
                    return cb(null, []);
                }
                this._log.error(
                    'error getting list of offsets from zookeeper', {
                        topic, partition, offsetType,
                        error: err.message,
                    });
                return cb(err);
            }
            return async.mapLimit(
                labels, 10,
                (label, done) => this._readOffset(
                    topic, partition, offsetType, label, (err, offset) => {
                        if (err) {
                            return done(err);
                        }
                        return done(null, { label, offset });
                    }),
                cb);
        });
    }

    /**
     * Fetch latest consumable offset from topic
     *
     * @param {node-rdkafka.Client} kafkaClient - producer or consumer
     * @param {string} topic - topic name
     * @param {number} partition - partition number to fetch latest
     * consumable offset from
     * @param {function} cb - callback: cb(err, offset)
     * @return {undefined}
     */
    _getLatestTopicOffset(kafkaClient, topic, partition, cb) {
        this._log.debug(
            'querying latest topic partition offset from kafka client', {
                topic, partition,
            });
        kafkaClient.queryWatermarkOffsets(
            topic, partition, 10000, (err, offsets) => {
                if (err) {
                    this._log.error(
                        'error getting latest topic partition offset', {
                            topic, partition,
                            errorCode: err, // kafka error does not have a
                            // message field
                        });
                    return cb(errors.InternalError);
                }
                // high watermark is last message pushed and consumable
                return cb(null, offsets.highOffset);
            });
    }

    /**
     * Publish consumer backlog for a topic and consumer group, as a
     * tuple (topicOffset,consumerGroupOffset) for each partition, in
     * zookeeper.
     *
     * The consumer lag at this point in time can later be checked
     * with KafkaBacklogMetrics.checkConsumerLag().
     *
     * @param {node-rdkafka.Consumer} consumer - kafka consumer
     * @param {string} topic - topic name
     * @param {string} groupId - consumer group
     * @param {function} cb - callback: cb(err)
     * @return {undefined}
     */
    publishConsumerBacklog(consumer, topic, groupId, cb) {
        let consumerOffsets;
        this._log.debug('publishing kafka consumer backlog offsets', {
            topic, groupId,
        });
        try {
            // NOTE: for an unknown reason, in some cases all
            // partitions are published but some do not have a set
            // consumer offset yet, so pre-filter here.
            consumerOffsets = consumer.position()
                .filter(p => p.topic === topic && p.offset !== undefined);
        } catch (err) {
            this._log.error('error getting consumer current offsets', {
                topic, groupId,
                error: err.message,
            });
            if (cb) {
                return process.nextTick(cb);
            }
            return undefined;
        }
        const topicOffsets = [];
        return async.eachLimit(consumerOffsets, 10, (p, done) => {
            this._getLatestTopicOffset(
                consumer, topic, p.partition, (err, topicOffset) => {
                    if (err) {
                        return done(err);
                    }
                    topicOffsets.push({ partition: p.partition,
                                        offset: topicOffset });
                    return async.parallel([
                        done => this._publishOffset(
                            topic, p.partition, p.offset,
                            'consumers', groupId, done),
                        done => this._publishOffset(
                            topic, p.partition, topicOffset,
                            'topic', null, done),
                    ], done);
                });
        }, err => {
            if (!err) {
                this._log.info(
                    'published consumer and topic offsets to zookeeper', {
                        topic, groupId,
                        consumerOffsets,
                        topicOffsets,
                    });
            }
            return cb(err);
        });
    }

    /**
     * Create a snapshot of current topic offsets for the chosen
     * topic, store them in the given snapshot name.
     *
     * The snapshot can be used later to check consumer progress, with
     * KafkaBacklogMetrics.checkConsumerProgress().
     *
     * @param {node-rdkafka.Client} kafkaClient - kafka producer or consumer
     * @param {string} topic - topic name
     * @param {string} snapshotName - snapshot name (to be referred to
     * in checkConsumerProgress())
     * @param {function} cb - callback: cb(err)
     * @return {undefined}
     */
    snapshotTopicOffsets(kafkaClient, topic, snapshotName, cb) {
        this._log.debug('snapshotting topic offsets to zookeeper', {
            topic, snapshotName,
        });
        kafkaClient.getMetadata({ topic, timeout: 10000 }, (err, res) => {
            if (err) {
                this._log.error('error getting metadata for topic', {
                    topic,
                    errorCode: err,
                });
                return cb(errors.InternalError);
            }
            const topicMd = res.topics.find(t => t.name === topic);
            if (!topicMd) {
                this._log.info(
                    'skipped topic offsets snapshot: topic metadata not found',
                    { topic,
                      snapshotName,
                    });
                return process.nextTick(cb);
            }
            const topicOffsets = [];
            return async.each(topicMd.partitions, (partMd, done) => {
                const partition = partMd.id;
                this._getLatestTopicOffset(
                    kafkaClient, topic, partition, (err, topicOffset) => {
                        if (err) {
                            return done(err);
                        }
                        topicOffsets.push({ partition, offset: topicOffset });
                        return this._publishOffset(
                            topic, partition, topicOffset,
                            'snapshots', snapshotName, done);
                    });
            }, err => {
                if (!err) {
                    this._log.info('snapshotted topic offsets to zookeeper', {
                        topic,
                        snapshotName,
                        topicOffsets,
                    });
                }
                return cb(err);
            });
        });
    }

    _checkConsumerOffsetsGeneric(topic, groupId, maxLag, snapshotName, cb) {
        let checkInfo = undefined;
        const partitionsZkPath = this._getPartitionsOffsetsZkPath(topic);
        this._zookeeper.getChildren(partitionsZkPath, (err, partitions) => {
            if (err) {
                if (err.getCode() === zookeeper.Exception.NO_NODE) {
                    this._log.debug('no topic offset published yet', {
                        topic, zkPath: partitionsZkPath,
                    });
                    return cb();
                }
                this._log.error(
                    'error getting list of topic partitions from zookeeper', {
                        topic, zkPath: partitionsZkPath,
                        error: err.message,
                    });
                return cb(err);
            }
            return async.eachSeries(partitions, (partition, partitionDone) => {
                let consumerOffsets;
                let targetOffset;
                async.waterfall([
                    next => {
                        if (groupId) {
                            // read consumer group offset of particular group
                            this._readOffset(topic, partition,
                                             'consumers', groupId, next);
                        } else {
                            // read all consumer groups offsets
                            this._readAllOffsets(topic, partition,
                                                 'consumers', next);
                        }
                    },
                    (offsets, next) => {
                        consumerOffsets = offsets;
                        if (snapshotName) {
                            // read offset from previous snapshot
                            this._readOffset(topic, partition, 'snapshots',
                                             snapshotName, next);
                        } else {
                            // read latest topic partition offset
                            this._readOffset(topic, partition, 'topic',
                                             null, next);
                        }
                    },
                    (offset, next) => {
                        if (offset === OutdatedOffset) {
                            // topic offset is outdated, skip check
                            return next();
                        }
                        targetOffset = offset;
                        if (!Array.isArray(consumerOffsets)) {
                            consumerOffsets = [{
                                label: groupId,
                                offset: consumerOffsets,
                            }];
                        }
                        // remove outdated consumer offsets
                        consumerOffsets = consumerOffsets.filter(
                            consumerOffsetInfo =>
                                consumerOffsetInfo.offset !== OutdatedOffset);
                        const partitionNumber = Number.parseInt(partition, 10);
                        consumerOffsets.forEach(consumerOffsetInfo => {
                            let lag = targetOffset - consumerOffsetInfo.offset;
                            if (lag < 0) {
                                lag = 0;
                            }
                            const info = {
                                topic,
                                partition: partitionNumber,
                                groupId: consumerOffsetInfo.label,
                                consumerOffset: consumerOffsetInfo.offset,
                                lag, maxLag,
                            };
                            if (snapshotName) {
                                info.snapshotName = snapshotName;
                                info.snapshotOffset = targetOffset;
                            } else {
                                info.topicOffset = targetOffset;
                            }
                            this._log.debug('lag computed for consumer/topic',
                                            info);
                            if (lag > maxLag && !checkInfo) {
                                checkInfo = info;
                            }
                        });
                        if (checkInfo) {
                            return next(CheckConditionError);
                        }
                        return next();
                    },
                ], partitionDone);
            }, err => {
                if (err) {
                    if (err === CheckConditionError) {
                        return cb(null, checkInfo);
                    }
                    if (err === NoNodeError) {
                        // This might happen if a snapshot was
                        // requested but does not exist, which is a
                        // normal situation if no message has been
                        // sent to the snapshotted topic. We can
                        // consider "everything" has been processed
                        // then and satisfy the check.
                        return cb();
                    }
                    return cb(err);
                }
                return cb();
            });
        });
    }

    /**
     * Check whether the given consumer group lags beyond the maximum
     * lag allowed. The lag is defined as the number of messages
     * published to some topic partition, but not yet consumed by the
     * group.
     *
     * @param {string} topic - topic name
     * @param {string} [groupId] - consumer group (or null to check
     * all consumer groups that have published metrics)
     * @param {number} maxLag - maximum lag allowed per partition, as
     * the difference between topic and consumer group offset (0 means
     * no lag is allowed)
     * @param {function} cb - callback:
     * - cb(): success and lag is less than maxLag for all partitions
     * - cb(null, checkInfo): lag is above maxLag for at least one
     *   partition, and checkInfo is an object containing info about
     *   the first partition where the lag is too high
     * - cb(err): an error occurred
     * @return {undefined}
     */
    checkConsumerLag(topic, groupId, maxLag, cb) {
        this._log.debug('checking consumer lag', {
            topic, groupId, maxLag,
        });
        return this._checkConsumerOffsetsGeneric(
            topic, groupId, maxLag, null, cb);
    }

    /**
     * Check whether the given consumer group has made progress at
     * least up to the given snapshot previously taken via
     * KafkaBacklogMetrics.snapshotTopicOffsets(). The progress is
     * defined as the stored consumer offset being greater or equal
     * than the snapshot offsets, for all partitions.
     *
     * @param {string} topic - topic name
     * @param {string} [groupId] - consumer group (or null to check
     * all consumer groups that have published metrics)
     * @param {string} snapshotName - name of snapshot created earlier
     * with KafkaBacklogMetrics.snapshotTopicOffsets()
     * @param {function} cb - callback:
     * - cb(): success and consumer group has progressed beyond the
     *   snapshot offsets for all partitions
     * - cb(null, checkInfo): consumer group position is behind the
     *   snapshot offset for at least one partition, and checkInfo is
     *   an object containing info about the first partition where the
     *   consumer is behind the snapshot offset
     * - cb(err): an error occurred
     * @return {undefined}
     */
    checkConsumerProgress(topic, groupId, snapshotName, cb) {
        this._log.debug('checking consumer progress', {
            topic, groupId, snapshotName,
        });
        return this._checkConsumerOffsetsGeneric(
            topic, groupId, 0, snapshotName, cb);
    }
}

module.exports = KafkaBacklogMetrics;
