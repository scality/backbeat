'use strict';

const async = require('async');
const { KafkaConsumer } = require('node-rdkafka');
const Logger = require('werelogs').Logger;
const { errors, jsutil } = require('arsenal');

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const { withTopicPrefix } = require('../../../lib/util/topic');
const NotificationConfigManager = require('../NotificationConfigManager');
const configUtil = require('../utils/config');
const { buildDeliveryKey } = require('../utils/deliveryKey');

const CONNECT_TIMEOUT_MS = 30000;
const METADATA_TIMEOUT_MS = 10000;
const WATERMARK_TIMEOUT_MS = 10000;
const COMMITTED_TIMEOUT_MS = 10000;
const CONSUME_BATCH_SIZE = 100;
const EMPTY_BATCH_SLEEP_MS = 500;
const MAX_EMPTY_BATCHES = 120;
const MATCH_CONCURRENCY = 10;

/**
 * @class DeliveryTopicDrainer
 *
 * @classdesc Drains the internal notification topics of the old
 * per-destination pipeline into the delivery topic consumed by the
 * delivery worker pool, as addressed records (old payload plus
 * destinationId and configurationId).
 *
 * It is meant to run once during cutover, with the queue populator and
 * every queue processor stopped, so that the topics it reads are not
 * moving. It reads the committed offsets of the old consumer groups but
 * never writes them: a rerun starts from the same place and produces
 * duplicates, and a rollback to the old pipeline resumes exactly where
 * it stopped. Duplicates are acceptable on the delivery topic, gaps are
 * not, so any error (an unreadable bucket configuration, a failed
 * produce) aborts the drain instead of stepping over the record.
 */
class DeliveryTopicDrainer {

    /**
     * @constructor
     * @param {Object} params - params object
     * @param {Object} params.kafkaConfig - kafka configuration object
     * @param {String} params.kafkaConfig.hosts - list of kafka brokers
     * @param {Object} [params.mongoConfig] - mongodb connection config,
     *   when absent the zookeeper config backend is used
     * @param {Object} [params.zkConfig] - zookeeper configuration object
     * @param {Object} params.notifConfig - notification configuration
     *   object, the same "extensions.notification" block the populator
     *   and the queue processors read
     * @param {String} [params.replayId] - suffix of the throwaway
     *   consumer group, defaults to the process id
     * @param {Object} [params.consumer] - node-rdkafka consumer, for tests
     * @param {Object} [params.producer] - BackbeatProducer, for tests
     * @param {Object} [params.bnConfigManager] - config manager, for tests
     * @param {Number} [params.batchSize] - records per consume call
     * @param {Number} [params.emptyBatchSleepMs] - pause before rechecking
     *   partition positions after an empty batch
     * @param {Number} [params.maxEmptyBatches] - how many consecutive
     *   fruitless batches to accept before giving up on a partition
     * @param {Number} [params.matchConcurrency] - how many records of a
     *   batch to match against bucket configurations in parallel
     * @param {Object} [params.logger] - werelogs logger
     */
    constructor(params) {
        const {
            kafkaConfig, mongoConfig, zkConfig, notifConfig, replayId,
            consumer, producer, bnConfigManager, batchSize,
            emptyBatchSleepMs, maxEmptyBatches, matchConcurrency, logger,
        } = params;
        this.kafkaConfig = kafkaConfig;
        this.mongoConfig = mongoConfig;
        this.zkConfig = zkConfig;
        this.notifConfig = notifConfig;
        this.replayId = replayId || String(process.pid);
        this.groupId = `bn-replay-${this.replayId}`;

        this.bnConfigManager = bnConfigManager || null;
        this._consumer = consumer || null;
        this._producer = producer || null;
        // injected clients belong to the caller, we neither connect nor
        // close them
        this._ownsConsumer = !consumer;
        this._ownsProducer = !producer;

        this._batchSize = batchSize || CONSUME_BATCH_SIZE;
        this._emptyBatchSleepMs = emptyBatchSleepMs === undefined ?
            EMPTY_BATCH_SLEEP_MS : emptyBatchSleepMs;
        this._maxEmptyBatches = maxEmptyBatches || MAX_EMPTY_BATCHES;
        this._matchConcurrency = matchConcurrency || MATCH_CONCURRENCY;

        this.totals = { drained: 0, produced: 0, skipped: 0 };
        this.logger = logger ||
            new Logger('Backbeat:Notification:DeliveryTopicDrainer');
    }

    /**
     * Resolves where a partition has to be drained from, out of the old
     * consumer group offset and the partition watermarks.
     *
     * A group that never committed on the partition, or that committed
     * below the low watermark because retention already dropped those
     * records, restarts at the low watermark: that is the earliest thing
     * we can still deliver.
     *
     * @param {Object} bounds - partition bounds
     * @param {Number} [bounds.committedOffset] - offset committed by the
     *   old consumer group, negative or missing when there is none
     * @param {Number} bounds.lowOffset - low watermark
     * @param {Number} bounds.highOffset - high watermark, the offset the
     *   next record would get
     * @return {Object} object with the resolved startOffset, a skip flag
     *   telling there is nothing to drain, and a reason to log
     */
    static resolveStartOffset(bounds) {
        const { committedOffset, lowOffset, highOffset } = bounds;
        let startOffset = committedOffset;
        let reason = 'resuming at old consumer group offset';
        if (typeof startOffset !== 'number' ||
            !Number.isFinite(startOffset) || startOffset < 0) {
            startOffset = lowOffset;
            reason = 'old consumer group has no committed offset';
        } else if (startOffset < lowOffset) {
            startOffset = lowOffset;
            reason = 'old consumer group offset is below the low watermark, ' +
                'records were already dropped by retention';
        }
        if (startOffset >= highOffset) {
            return { startOffset, skip: true, reason: 'nothing left to drain' };
        }
        return { startOffset, skip: false, reason };
    }

    /**
     * Initializes the config manager, the delivery topic producer and the
     * consumer used to read the old topics
     *
     * @param {Function} done - callback
     * @return {undefined}
     */
    start(done) {
        async.series([
            next => this._setupNotificationConfigManager(next),
            next => this._setupProducer(next),
            next => this._setupConsumer(next),
        ], err => {
            if (err) {
                this.logger.error('error starting delivery topic drainer', {
                    method: 'DeliveryTopicDrainer.start',
                    error: err.message,
                });
                return done(err);
            }
            return done();
        });
    }

    /**
     * Initializes the NotificationConfigManager
     *
     * @param {Function} done - callback
     * @return {undefined}
     */
    _setupNotificationConfigManager(done) {
        if (this.bnConfigManager) {
            return process.nextTick(done);
        }
        try {
            this.bnConfigManager = new NotificationConfigManager({
                mongoConfig: this.mongoConfig,
                bucketMetastore: this.notifConfig.bucketMetastore,
                maxCachedConfigs: this.notifConfig.maxCachedConfigs,
                zkConfig: this.zkConfig,
                zkPath: this.notifConfig.zookeeperPath,
                zkConcurrency: this.notifConfig.zookeeperOpConcurrency,
                logger: this.logger,
            });
            return this.bnConfigManager.setup(done);
        } catch (err) {
            return done(err);
        }
    }

    /**
     * Creates the single producer shared by every destination. It is
     * bound to the delivery topic and applies the topic prefix itself.
     *
     * @param {Function} done - callback
     * @return {undefined}
     */
    _setupProducer(done) {
        if (this._producer) {
            return process.nextTick(done);
        }
        const topic = this._getDeliveryTopic();
        if (!topic) {
            return process.nextTick(() => done(errors.InternalError
                .customizeDescription(
                    'missing extensions.notification.deliveryPool.topic')));
        }
        const doneOnce = jsutil.once(done);
        this._producer = new BackbeatProducer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic,
            compressionType: this.kafkaConfig.compressionType,
            requiredAcks: this.kafkaConfig.requiredAcks,
        });
        this._producer.on('error', err => {
            this.logger.error('error with delivery topic producer', {
                method: 'DeliveryTopicDrainer._setupProducer',
                error: err.message,
            });
            doneOnce(err);
        });
        this._producer.once('ready', () => doneOnce());
        return undefined;
    }

    /**
     * Creates the consumer that reads the old topics. It uses a throwaway
     * group and has every form of offset writing disabled: the old groups
     * must be left exactly as the queue processors left them.
     *
     * @param {Function} done - callback
     * @return {undefined}
     */
    _setupConsumer(done) {
        if (this._consumer) {
            return process.nextTick(done);
        }
        this._consumer = new KafkaConsumer({
            'metadata.broker.list': this.kafkaConfig.hosts,
            'group.id': this.groupId,
            'enable.auto.commit': false,
            'enable.auto.offset.store': false,
            'allow.auto.create.topics': false,
            'metadata.max.age.ms': 5000,
        }, {});
        this._consumer.on('event.error', err =>
            this.logger.error('rdkafka.error', { err }));
        return this._consumer.connect({ timeout: CONNECT_TIMEOUT_MS }, err => {
            if (err) {
                this.logger.error('error connecting replay consumer', {
                    method: 'DeliveryTopicDrainer._setupConsumer',
                    groupId: this.groupId,
                    error: err.message,
                });
                return done(err);
            }
            return done();
        });
    }

    _getDeliveryTopic() {
        return this.notifConfig.deliveryPool &&
            this.notifConfig.deliveryPool.topic;
    }

    /**
     * Drains every configured destination, one after the other
     *
     * @param {Function} done - callback: done(err, totals)
     * @return {undefined}
     */
    run(done) {
        const destinations = this.notifConfig.destinations || [];
        this.logger.info('starting notification delivery replay', {
            destinations: destinations.map(d => d.resource),
            deliveryTopic: this._getDeliveryTopic(),
            replayGroupId: this.groupId,
        });
        return async.eachSeries(destinations, (destination, next) =>
            this.drainDestination(destination, next), err => {
            if (err) {
                this.logger.error('notification delivery replay failed', {
                    method: 'DeliveryTopicDrainer.run',
                    error: err.message,
                    totals: this.totals,
                });
                return done(err);
            }
            this.logger.info('notification delivery replay complete',
                this.totals);
            return done(null, this.totals);
        });
    }

    /**
     * Drains the old topic of a single destination
     *
     * @param {Object} destination - destination config entry
     * @param {Function} done - callback
     * @return {undefined}
     */
    drainDestination(destination, done) {
        const groupIdPrefix = this.notifConfig.queueProcessor &&
            this.notifConfig.queueProcessor.groupId;
        if (!groupIdPrefix) {
            return done(errors.InternalError.customizeDescription(
                'missing extensions.notification.queueProcessor.groupId'));
        }
        const internalTopic = destination.internalTopic ||
            this.notifConfig.topic;
        if (!internalTopic) {
            return done(errors.InternalError.customizeDescription(
                `no internal topic configured for ${destination.resource}`));
        }
        // raw rdkafka consumption does not apply the topic prefix, unlike
        // BackbeatConsumer, so it is applied here
        const oldTopic = withTopicPrefix(internalTopic);
        const oldGroup = `${groupIdPrefix}-${destination.resource}`;
        return async.waterfall([
            next => this._getPartitions(oldTopic, next),
            (partitions, next) =>
                this._getDrainPlan(oldTopic, oldGroup, partitions, next),
            (plan, next) =>
                this._drainPartitions(destination, oldTopic, oldGroup, plan,
                    next),
        ], done);
    }

    /**
     * Lists the partitions of an old topic
     *
     * @param {String} oldTopic - prefixed topic name
     * @param {Function} done - callback: done(err, partitions)
     * @return {undefined}
     */
    _getPartitions(oldTopic, done) {
        return this._consumer.getMetadata({
            topic: oldTopic,
            timeout: METADATA_TIMEOUT_MS,
        }, (err, metadata) => {
            if (err) {
                this.logger.error('error getting metadata for old topic', {
                    method: 'DeliveryTopicDrainer._getPartitions',
                    oldTopic,
                    errorCode: err,
                });
                return done(errors.InternalError);
            }
            const topicMd = metadata.topics.find(t => t.name === oldTopic);
            if (!topicMd || topicMd.partitions.length === 0) {
                this.logger.info('old topic has no partitions, nothing to ' +
                    'drain', { oldTopic });
                return done(null, []);
            }
            return done(null, topicMd.partitions.map(p => p.id));
        });
    }

    /**
     * Builds the per partition drain plan: where to start, and where the
     * head was when the drain started. The head is captured once so that
     * the drain has a fixed target and terminates.
     *
     * @param {String} oldTopic - prefixed topic name
     * @param {String} oldGroup - old consumer group id
     * @param {Number[]} partitions - partition ids
     * @param {Function} done - callback: done(err, plan)
     * @return {undefined}
     */
    _getDrainPlan(oldTopic, oldGroup, partitions, done) {
        if (partitions.length === 0) {
            return process.nextTick(() => done(null, []));
        }
        return async.waterfall([
            next => this._readCommittedOffsets(oldTopic, oldGroup, partitions,
                next),
            (committedOffsets, next) => async.mapSeries(partitions,
                (partition, partitionDone) => this._planPartition(oldTopic,
                    partition, committedOffsets[partition], partitionDone),
                next),
        ], (err, plan) => {
            if (err) {
                return done(err);
            }
            plan.forEach(entry => this._logPlanEntry(oldTopic, oldGroup, entry));
            return done(null, plan.filter(entry => !entry.skip));
        });
    }

    /**
     * Captures the watermarks of one partition once, and resolves where
     * the drain starts from
     *
     * @param {String} oldTopic - prefixed topic name
     * @param {Number} partition - partition id
     * @param {Number} [committedOffset] - old consumer group offset
     * @param {Function} done - callback: done(err, planEntry)
     * @return {undefined}
     */
    _planPartition(oldTopic, partition, committedOffset, done) {
        return this._consumer.queryWatermarkOffsets(oldTopic, partition,
            WATERMARK_TIMEOUT_MS, (err, offsets) => {
                if (err) {
                    this.logger.error('error getting watermark offsets', {
                        method: 'DeliveryTopicDrainer._planPartition',
                        oldTopic,
                        partition,
                        errorCode: err,
                    });
                    return done(errors.InternalError);
                }
                const { lowOffset, highOffset } = offsets;
                const resolved = DeliveryTopicDrainer.resolveStartOffset({
                    committedOffset,
                    lowOffset,
                    highOffset,
                });
                return done(null, Object.assign({
                    partition,
                    committedOffset,
                    lowOffset,
                    headOffset: highOffset,
                }, resolved));
            });
    }

    _logPlanEntry(oldTopic, oldGroup, entry) {
        const info = {
            oldTopic,
            oldGroup,
            partition: entry.partition,
            committedOffset: entry.committedOffset,
            lowOffset: entry.lowOffset,
            headOffset: entry.headOffset,
            startOffset: entry.startOffset,
            reason: entry.reason,
        };
        if (typeof entry.committedOffset === 'number' &&
            entry.committedOffset >= 0 &&
            entry.committedOffset < entry.lowOffset) {
            this.logger.warn('records were lost before the replay', info);
        } else {
            this.logger.info('resolved replay start offset', info);
        }
    }

    /**
     * Reads the offsets committed by the old consumer group.
     *
     * In node-rdkafka 2.18 committed() is scoped to the group.id of the
     * client it is called on, so a client configured with the old group id
     * is the only way to see those offsets. That client only ever calls
     * committed(): it never subscribes, assigns or commits, so the old
     * group keeps its offsets and a rollback stays possible.
     *
     * @param {String} oldTopic - prefixed topic name
     * @param {String} oldGroup - old consumer group id
     * @param {Number[]} partitions - partition ids
     * @param {Function} done - callback: done(err, offsetsByPartition)
     * @return {undefined}
     */
    _readCommittedOffsets(oldTopic, oldGroup, partitions, done) {
        const toppars = partitions.map(partition => ({
            topic: oldTopic,
            partition,
        }));
        return this._withOffsetReader(oldGroup, (reader, next) =>
            reader.committed(toppars, COMMITTED_TIMEOUT_MS, next),
            (err, committedToppars) => {
                if (err) {
                    this.logger.error('error reading committed offsets of ' +
                        'the old consumer group', {
                        method: 'DeliveryTopicDrainer._readCommittedOffsets',
                        oldTopic,
                        oldGroup,
                        error: err.message,
                    });
                    return done(errors.InternalError.customizeDescription(
                        err.message));
                }
                const offsets = {};
                (committedToppars || []).forEach(tp => {
                    offsets[tp.partition] = tp.offset;
                });
                return done(null, offsets);
            });
    }

    /**
     * Runs fn against a consumer bound to the given group id. When a
     * consumer was injected it is reused as is, otherwise a short lived
     * one is connected and disconnected around the call.
     *
     * @param {String} groupId - consumer group id to read offsets of
     * @param {Function} fn - fn(reader, cb)
     * @param {Function} done - callback: done(err, result)
     * @return {undefined}
     */
    _withOffsetReader(groupId, fn, done) {
        if (!this._ownsConsumer) {
            return fn(this._consumer, done);
        }
        const reader = new KafkaConsumer({
            'metadata.broker.list': this.kafkaConfig.hosts,
            'group.id': groupId,
            'enable.auto.commit': false,
            'enable.auto.offset.store': false,
        }, {});
        return reader.connect({ timeout: CONNECT_TIMEOUT_MS }, connectErr => {
            if (connectErr) {
                return done(connectErr);
            }
            return fn(reader, (err, result) =>
                reader.disconnect(() => done(err, result)));
        });
    }

    /**
     * Assigns the partitions to drain at their start offsets and consumes
     * them until each one reaches the head captured in the plan
     *
     * @param {Object} destination - destination config entry
     * @param {String} oldTopic - prefixed topic name
     * @param {String} oldGroup - old consumer group id
     * @param {Object[]} plan - per partition drain plan
     * @param {Function} done - callback
     * @return {undefined}
     */
    _drainPartitions(destination, oldTopic, oldGroup, plan, done) {
        if (plan.length === 0) {
            this.logger.info('nothing to drain for destination', {
                destination: destination.resource,
                oldTopic,
                oldGroup,
            });
            return process.nextTick(done);
        }
        const state = new Map();
        plan.forEach(p => state.set(p.partition, {
            partition: p.partition,
            startOffset: p.startOffset,
            headOffset: p.headOffset,
            lastOffset: -1,
            position: -1,
            drained: 0,
            produced: 0,
            skipped: 0,
        }));
        this._consumer.assign(plan.map(p => ({
            topic: oldTopic,
            partition: p.partition,
            offset: p.startOffset,
        })));
        let emptyBatches = 0;
        const loop = () => {
            const pending = this._pendingPartitions(state);
            if (pending.length === 0) {
                return this._finishDestination(destination, oldTopic, oldGroup,
                    state, done);
            }
            if (emptyBatches >= this._maxEmptyBatches) {
                this.logger.error('gave up waiting for partitions to reach ' +
                    'their head offset', {
                    method: 'DeliveryTopicDrainer._drainPartitions',
                    oldTopic,
                    oldGroup,
                    pending: pending.map(s => ({
                        partition: s.partition,
                        lastOffset: s.lastOffset,
                        headOffset: s.headOffset,
                    })),
                });
                return done(errors.InternalError.customizeDescription(
                    `replay stalled on topic ${oldTopic}`));
            }
            return this._drainBatch(destination, oldTopic, state,
                (err, consumed) => {
                    if (err) {
                        return done(err);
                    }
                    emptyBatches = consumed > 0 ? 0 : emptyBatches + 1;
                    return setImmediate(loop);
                });
        };
        return loop();
    }

    _pendingPartitions(state) {
        return [...state.values()].filter(s => {
            if (s.lastOffset >= s.headOffset - 1) {
                return false;
            }
            // position() is the offset the consumer would read next, it
            // moves past records that consume() never returns
            return !(s.position >= 0 && s.position >= s.headOffset);
        });
    }

    _finishDestination(destination, oldTopic, oldGroup, state, done) {
        // destinations sharing one internal topic are drained from
        // different offsets, so drop the assignment and its fetch queue
        // before the next one assigns the same partitions
        this._consumer.unassign();
        state.forEach(s => {
            this.logger.info('drained partition', {
                oldTopic,
                oldGroup,
                partition: s.partition,
                startOffset: s.startOffset,
                headOffset: s.headOffset,
                drained: s.drained,
                produced: s.produced,
                skipped: s.skipped,
            });
        });
        this.logger.info('drained destination', {
            destination: destination.resource,
            oldTopic,
            oldGroup,
            partitions: state.size,
        });
        return done();
    }

    /**
     * Consumes one batch and produces every matching record of it to the
     * delivery topic. The next batch is only consumed once the delivery
     * reports of this one are in, which bounds memory and keeps the
     * counters honest.
     *
     * @param {Object} destination - destination config entry
     * @param {String} oldTopic - prefixed topic name
     * @param {Map} state - per partition drain state
     * @param {Function} done - callback: done(err, consumedCount)
     * @return {undefined}
     */
    _drainBatch(destination, oldTopic, state, done) {
        return this._consumer.consume(this._batchSize, (err, records) => {
            if (err) {
                this.logger.error('error consuming from old topic', {
                    method: 'DeliveryTopicDrainer._drainBatch',
                    oldTopic,
                    errorCode: err,
                });
                return done(errors.InternalError);
            }
            if (!records || records.length === 0) {
                return this._refreshPositions(oldTopic, state, done);
            }
            return this._processBatch(destination, records, state, batchErr => {
                if (batchErr) {
                    return done(batchErr);
                }
                return done(null, records.length);
            });
        });
    }

    /**
     * Waits a little then rereads the consumer positions, so that a
     * partition whose remaining offsets yield no record still gets to the
     * head instead of looping forever
     *
     * @param {String} oldTopic - prefixed topic name
     * @param {Map} state - per partition drain state
     * @param {Function} done - callback: done(err, consumedCount)
     * @return {undefined}
     */
    _refreshPositions(oldTopic, state, done) {
        return setTimeout(() => {
            let positions;
            try {
                positions = this._consumer.position() || [];
            } catch (err) {
                this.logger.error('error reading consumer positions', {
                    method: 'DeliveryTopicDrainer._refreshPositions',
                    oldTopic,
                    error: err.message,
                });
                return done(errors.InternalError.customizeDescription(
                    err.message));
            }
            positions.filter(p => p.topic === oldTopic).forEach(p => {
                const partitionState = state.get(p.partition);
                if (partitionState && typeof p.offset === 'number' &&
                    p.offset >= 0) {
                    partitionState.position = p.offset;
                }
            });
            return done(null, 0);
        }, this._emptyBatchSleepMs);
    }

    _processBatch(destination, records, state, done) {
        return async.mapLimit(records, this._matchConcurrency,
            (record, next) => this._matchRecord(destination, record, next),
            (err, messages) => {
                if (err) {
                    return done(err);
                }
                const toProduce = [];
                records.forEach((record, i) => {
                    const partitionState = state.get(record.partition);
                    if (partitionState) {
                        partitionState.drained++;
                        if (record.offset > partitionState.lastOffset) {
                            partitionState.lastOffset = record.offset;
                        }
                    }
                    this.totals.drained++;
                    if (messages[i]) {
                        toProduce.push(messages[i]);
                        this.totals.produced++;
                        if (partitionState) {
                            partitionState.produced++;
                        }
                    } else {
                        this.totals.skipped++;
                        if (partitionState) {
                            partitionState.skipped++;
                        }
                    }
                });
                if (toProduce.length === 0) {
                    return process.nextTick(done);
                }
                return this._producer.send(toProduce, sendErr => {
                    if (sendErr) {
                        this.logger.error('error producing to delivery topic', {
                            method: 'DeliveryTopicDrainer._processBatch',
                            destination: destination.resource,
                            error: sendErr.message,
                        });
                        return done(sendErr);
                    }
                    return done();
                });
            });
    }

    /**
     * Rematches one old record against the bucket notification
     * configuration, the way the queue processor of this destination
     * would have, and turns it into an addressed delivery record.
     *
     * Old records carry no configurationId, it is only known once the
     * matching rule is found again here.
     *
     * @param {Object} destination - destination config entry
     * @param {Object} record - kafka record from the old topic
     * @param {Function} done - callback: done(err, message), message being
     *   null when the record has nothing to deliver for this destination
     * @return {undefined}
     */
    _matchRecord(destination, record, done) {
        let entry;
        try {
            entry = JSON.parse(record.value);
        } catch (err) {
            this.logger.error('error parsing JSON entry, skipping record', {
                method: 'DeliveryTopicDrainer._matchRecord',
                partition: record.partition,
                offset: record.offset,
                error: err.message,
            });
            return process.nextTick(() => done(null, null));
        }
        const { bucket, key } = entry;
        return this.bnConfigManager.getConfig(bucket, (err, bnConfig) => {
            if (err) {
                // a record we cannot match is a record we cannot decide
                // about, and skipping it would be a gap, so give up and
                // let the operator rerun the replay
                this.logger.error('error getting notification configuration', {
                    method: 'DeliveryTopicDrainer._matchRecord',
                    bucket,
                    key,
                    error: err.message,
                });
                return done(err);
            }
            if (!bnConfig || Object.keys(bnConfig).length === 0 ||
                !bnConfig.notificationConfiguration) {
                return done(null, null);
            }
            const queueConfig =
                bnConfig.notificationConfiguration.queueConfig.filter(
                    c => c.queueArn.split(':').pop() === destination.resource);
            if (!queueConfig.length) {
                return done(null, null);
            }
            const destConfig = {
                bucket,
                notificationConfiguration: { queueConfig },
            };
            const { isValid, matchingConfig } =
                configUtil.validateEntry(destConfig, entry);
            if (!isValid) {
                return done(null, null);
            }
            entry.destinationId = destination.resource;
            entry.configurationId = matchingConfig.id;
            return done(null, {
                // the populator's publish() path url-encodes record keys,
                // producing here goes straight through BackbeatProducer so
                // the same encoding has to be applied, otherwise records of
                // one object would land on two different partitions
                key: encodeURIComponent(
                    buildDeliveryKey(destination, bucket, key)),
                message: JSON.stringify(entry),
            });
        });
    }

    /**
     * Disconnects the clients this drainer created
     *
     * @param {Function} done - callback
     * @return {undefined}
     */
    stop(done) {
        return async.series([
            next => {
                if (!this._consumer || !this._ownsConsumer) {
                    return next();
                }
                return this._consumer.disconnect(() => next());
            },
            next => {
                if (!this._producer || !this._ownsProducer) {
                    return next();
                }
                return this._producer.close(() => next());
            },
        ], () => done());
    }
}

module.exports = DeliveryTopicDrainer;
