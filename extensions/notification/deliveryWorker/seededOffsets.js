'use strict';

const { KafkaConsumer } = require('node-rdkafka');
const { errors, jsutil } = require('arsenal');

const { withTopicPrefix } = require('../../../lib/util/topic');

const CONNECT_TIMEOUT_MS = 30000;
const METADATA_TIMEOUT_MS = 10000;
const COMMITTED_TIMEOUT_MS = 10000;

/**
 * Runs fn against a consumer bound to the given group id. An injected
 * consumer is used as it is, otherwise a short lived one is connected and
 * disconnected around the call.
 *
 * The consumer never subscribes, never assigns and never commits, so the
 * group stays Empty and keeps the offsets that were seeded into it.
 *
 * @param {Object} params - assertSeededOffsets params
 * @param {Function} fn - fn(consumer, cb)
 * @param {Function} done - callback: done(err, result)
 * @return {undefined}
 */
function _withOffsetReader(params, fn, done) {
    if (params.consumer) {
        return fn(params.consumer, done);
    }
    const reader = new KafkaConsumer({
        'metadata.broker.list': params.kafkaConfig.hosts,
        'group.id': params.groupId,
        'enable.auto.commit': false,
        'enable.auto.offset.store': false,
    }, {});
    reader.on('event.error', err =>
        params.logger.error('rdkafka.error', { err }));
    return reader.connect({ timeout: CONNECT_TIMEOUT_MS }, connectErr => {
        if (connectErr) {
            return done(connectErr);
        }
        return fn(reader, (err, result) =>
            reader.disconnect(() => done(err, result)));
    });
}

/**
 * Lists the partitions of the delivery topic
 *
 * @param {Object} consumer - connected consumer
 * @param {String} topic - prefixed topic name
 * @param {Function} done - callback: done(err, partitions)
 * @return {undefined}
 */
function _getPartitions(consumer, topic, done) {
    return consumer.getMetadata({
        topic,
        timeout: METADATA_TIMEOUT_MS,
    }, (err, metadata) => {
        if (err) {
            return done(errors.InternalError.customizeDescription(
                `error getting metadata for topic ${topic}: ` +
                `${err.message || err}`));
        }
        const topicMd = (metadata.topics || []).find(t => t.name === topic);
        if (!topicMd || topicMd.partitions.length === 0) {
            return done(errors.InternalError.customizeDescription(
                `topic ${topic} has no partitions`));
        }
        return done(null, topicMd.partitions.map(p => p.id));
    });
}

/**
 * Assert that a pre-seeded consumer group still has its offsets
 *
 * An empty group's offsets expire after the broker's
 * offsets.retention.minutes, counted from when the group became empty. A
 * worker joining a group whose pre-seed expired would start from the
 * delivery topic's low watermark, because fromOffset is 'earliest', and
 * replay everything the previous generation already delivered.
 *
 * @param {Object} params - params
 * @param {Object} params.kafkaConfig - kafka configuration object
 * @param {String} params.topic - unprefixed delivery topic name
 * @param {String} params.groupId - consumer group the worker will join
 * @param {Object} [params.barriers] - barrier offsets by partition
 * @param {Logger} params.logger - werelogs logger
 * @param {Object} [params.consumer] - node-rdkafka consumer, for tests
 * @param {Function} done - callback: done(err)
 * @return {undefined}
 */
function assertSeededOffsets(params, done) {
    const { topic, groupId, barriers, logger } = params;
    const prefixedTopic = withTopicPrefix(topic);
    const doneOnce = jsutil.once(done);
    return _withOffsetReader(params, (consumer, next) =>
        _getPartitions(consumer, prefixedTopic, (metaErr, partitions) => {
            if (metaErr) {
                return next(metaErr);
            }
            const toppars = partitions.map(partition => ({
                topic: prefixedTopic,
                partition,
            }));
            return consumer.committed(toppars, COMMITTED_TIMEOUT_MS,
                (committedErr, committedToppars) => {
                    if (committedErr) {
                        return next(errors.InternalError.customizeDescription(
                            'error reading the committed offsets of group ' +
                            `${groupId}: ` +
                            `${committedErr.message || committedErr}`));
                    }
                    return next(null, { partitions, committedToppars });
                });
        }),
    (err, result) => {
        if (err) {
            return doneOnce(err);
        }
        const offsets = {};
        (result.committedToppars || []).forEach(tp => {
            offsets[tp.partition] = tp.offset;
        });
        const unseeded = result.partitions.filter(partition => {
            const offset = offsets[partition];
            return typeof offset !== 'number' || !Number.isFinite(offset) ||
                offset < 0;
        });
        if (unseeded.length > 0) {
            return doneOnce(errors.InternalError.customizeDescription(
                `consumer group ${groupId} has no committed offset on ` +
                `partitions ${unseeded.join(', ')} of topic ${prefixedTopic}: ` +
                'the group was never pre-seeded or its offsets expired, and ' +
                'joining it would replay the topic. Run ' +
                'notificationWorkgroupCutover preseed'));
        }
        const behind = result.partitions
            .filter(partition => barriers &&
                barriers[partition] !== undefined &&
                offsets[partition] < barriers[partition])
            .map(partition => ({
                partition,
                barrier: barriers[partition],
                committed: offsets[partition],
            }));
        if (behind.length > 0) {
            // duplicates rather than gaps: the records between the committed
            // offset and the barrier were already handled by the previous
            // generation and will be delivered again
            logger.warn('consumer group is behind its cutover barrier', {
                method: 'assertSeededOffsets',
                groupId,
                topic: prefixedTopic,
                partitions: behind,
            });
        }
        logger.info('consumer group offsets are seeded', {
            method: 'assertSeededOffsets',
            groupId,
            topic: prefixedTopic,
            partitions: result.partitions.length,
        });
        return doneOnce();
    });
}

module.exports = {
    assertSeededOffsets,
};
