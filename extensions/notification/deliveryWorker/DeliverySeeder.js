'use strict';

const fs = require('fs');
const async = require('async');
const { KafkaConsumer, CODES } = require('node-rdkafka');
const { errors } = require('arsenal');

const ZookeeperManager = require('../../../lib/clients/ZookeeperManager');
const safeJsonParse = require('../../../lib/util/safeJsonParse');
const { withTopicPrefix } = require('../../../lib/util/topic');
const {
    buildGroupId,
    buildWatermarksPath,
    validateWorkgroupsDoc,
    workgroupIdForDestination,
} = require('../utils/workgroups');

const CONNECT_TIMEOUT_MS = 30000;
const DEFAULT_TIMEOUT_MS = 10000;

// commitSync on a group that has live members: the groups being seeded must
// be empty, which is the case between the stop and the start of a container
// swap
const GROUP_HAS_MEMBERS_CODES = [
    CODES.ERRORS.ERR_ILLEGAL_GENERATION,
    CODES.ERRORS.ERR_UNKNOWN_MEMBER_ID,
    CODES.ERRORS.ERR_REBALANCE_IN_PROGRESS,
];

// where the previous generation's document is kept, so a later seeding knows
// which group owned which destination
function buildHistoryPath(zkPath, generation) {
    return `${zkPath}/history/gen${generation}`;
}

/**
 * @class DeliverySeeder
 *
 * @classdesc Seeds the consumer groups of a delivery pool generation before
 * its workers start, for the deployment model where every processor or
 * worker container is stopped and the new ones started in one run.
 *
 * A new group with no committed offset would start at the oldest retained
 * record and deliver everything again, so each group is committed at the
 * lowest offset any destination it owns had been served up to, per
 * partition. Each destination's own offset is kept as a watermark in
 * zookeeper: the worker skips a matching record below the watermark of its
 * destination, so nothing is delivered twice, and a destination that was
 * behind receives its backlog.
 *
 * Sources are either the per-destination queue processor groups (migration
 * from today's processors) or the groups of the previous generation (a
 * workgroup layout change).
 */
class DeliverySeeder {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.kafkaConfig - kafka configuration object
     * @param {Object} params.zkConfig - the config.zookeeper block
     * @param {Object} params.notifConfig - the extensions.notification block
     * @param {Object} params.options - parsed command line options
     * @param {Logger} params.logger - werelogs logger
     * @param {Object} [params.zkClient] - zookeeper client, for tests
     * @param {Object} [params.consumer] - node-rdkafka consumer used for
     *   topic metadata and watermarks, for tests
     * @param {Function} [params.groupClientFactory] - (groupId) to a consumer
     *   bound to that group, for tests
     */
    constructor(params) {
        this._kafkaConfig = params.kafkaConfig;
        this._zkConfig = params.zkConfig;
        this._notifConfig = params.notifConfig;
        this._deliveryPoolConfig = params.notifConfig &&
            params.notifConfig.deliveryPool;
        this._options = params.options || {};
        this._log = params.logger;
        this._timeout = this._options.timeout !== undefined ?
            Number(this._options.timeout) : DEFAULT_TIMEOUT_MS;
        this._zkPath = this._deliveryPoolConfig &&
            this._deliveryPoolConfig.workgroups &&
            this._deliveryPoolConfig.workgroups.zookeeperPath;
        this._zkClient = params.zkClient || null;
        this._ownsZkClient = !params.zkClient;
        this._consumer = params.consumer || null;
        this._ownsConsumer = !params.consumer;
        this._groupClientFactory = params.groupClientFactory ||
            (groupId => this._defaultGroupClient(groupId));
    }

    /**
     * Seeds generation N of the workgroups from today's per-destination
     * queue processor groups
     *
     * @param {Function} done - callback: done(err, result)
     * @return {undefined}
     */
    seedFromProcessors(done) {
        const generation = Number(this._options.generation);
        if (!Number.isInteger(generation) || generation < 1) {
            return process.nextTick(() => done(
                errors.InvalidArgument.customizeDescription(
                    '--generation <n> is required and must be a positive ' +
                    'integer')));
        }
        const queueProcessor = this._notifConfig.queueProcessor;
        if (!queueProcessor || !queueProcessor.groupId) {
            return process.nextTick(() => done(
                errors.InvalidArgument.customizeDescription(
                    'extensions.notification.queueProcessor.groupId is not ' +
                    'configured, so the processor groups cannot be named')));
        }
        return this._seed({
            generation,
            sourceOf: destinationId => [
                `${queueProcessor.groupId}-${destinationId}`,
            ],
            sourceLabel: 'processor group',
        }, done);
    }

    /**
     * Seeds generation M of the workgroups from the groups of generation N
     *
     * @param {Function} done - callback: done(err, result)
     * @return {undefined}
     */
    seedFromGeneration(done) {
        const from = Number(this._options.from);
        const to = Number(this._options.to);
        if (!Number.isInteger(from) || from < 1 ||
            !Number.isInteger(to) || to <= from) {
            return process.nextTick(() => done(
                errors.InvalidArgument.customizeDescription(
                    '--from <n> and --to <m> are required, positive, and ' +
                    'm must be greater than n')));
        }
        return this._readPreviousDocument(from, (err, previousDoc, note) => {
            if (err) {
                return done(err);
            }
            const base = this._deliveryPoolConfig.groupId;
            let sourceOf;
            if (previousDoc) {
                sourceOf = destinationId => [buildGroupId(base,
                    workgroupIdForDestination(previousDoc, destinationId),
                    from)];
            } else {
                // without the previous document, a destination's owner is
                // unknown: the lowest offset over every previous group
                // never skips an undelivered record, at the cost of
                // duplicates for the destinations the fastest group served
                sourceOf = () => null;
            }
            return this._seed({
                generation: to,
                sourceOf,
                // the cutover tool records the groups a document replaced;
                // failing that, the previous generation is assumed to have
                // carried the same workgroup ids
                allSourcesOf: doc => (doc.previousGroups &&
                    doc.previousGroups.length > 0 ? doc.previousGroups :
                    doc.workgroups.map(wg => buildGroupId(base, wg.id, from))),
                sourceLabel: `generation ${from} group`,
                notes: note ? [note] : [],
            }, done);
        });
    }

    /**
     * Closes what this class opened
     *
     * @param {Function} done - callback
     * @return {undefined}
     */
    close(done) {
        return async.series([
            next => {
                if (this._consumer && this._ownsConsumer) {
                    return this._consumer.disconnect(() => next());
                }
                return process.nextTick(next);
            },
            next => {
                if (this._zkClient && this._ownsZkClient) {
                    this._zkClient.close();
                }
                return process.nextTick(next);
            },
        ], () => done());
    }

    /**
     * The shared seeding procedure
     *
     * @param {Object} plan - what to seed from
     * @param {Number} plan.generation - target generation
     * @param {Function} plan.sourceOf - (destinationId) to the source group
     *   ids of that destination, or null when they are not known
     * @param {Function} [plan.allSourcesOf] - (doc) to every source group id,
     *   used when sourceOf returns null
     * @param {String} plan.sourceLabel - how a source is named in the table
     * @param {String[]} [plan.notes] - notes to carry into the result
     * @param {Function} done - callback: done(err, result)
     * @return {undefined}
     */
    _seed(plan, done) {
        const configError = this._assertConfig();
        if (configError) {
            return process.nextTick(() => done(configError));
        }
        const result = {
            generation: plan.generation,
            topic: this._prefixedTopic(),
            partitions: [],
            groups: [],
            watermarks: {},
            watermarksPath: null,
            notes: (plan.notes || []).slice(),
        };
        const servedDestinations = this._servedDestinations();
        let doc = null;
        let partitions = [];
        let lows = {};
        let highs = {};
        const sourceOffsets = {};

        return async.series([
            next => this._readCurrentDocument((err, current) => {
                if (err) {
                    return next(err);
                }
                doc = current;
                if (!doc) {
                    return next(errors.InternalError.customizeDescription(
                        `no workgroups document at ${this._zkPath}: write ` +
                        'the generation to seed first'));
                }
                const { error } = validateWorkgroupsDoc(doc);
                if (error) {
                    return next(errors.InternalError.customizeDescription(
                        `the workgroups document at ${this._zkPath} is ` +
                        `invalid: ${error.message}`));
                }
                if (doc.generation !== plan.generation && !this._options.force) {
                    return next(errors.InvalidArgument.customizeDescription(
                        'the document in zookeeper is at generation ' +
                        `${doc.generation}, not ${plan.generation}; pass ` +
                        '--force to seed the generation you named anyway'));
                }
                return next();
            }),
            next => this._archiveDocument(doc, next),
            next => this._getPartitions((err, list) => {
                if (err) {
                    return next(err);
                }
                partitions = list;
                result.partitions = list;
                return next();
            }),
            next => this._queryWatermarks(partitions, (err, wm) => {
                if (err) {
                    return next(err);
                }
                lows = wm.lows;
                highs = wm.highs;
                return next();
            }),
            next => {
                // one read per distinct source group
                const groupsToRead = new Set();
                servedDestinations.forEach(destinationId => {
                    const sources = plan.sourceOf(destinationId) ||
                        plan.allSourcesOf(doc);
                    sources.forEach(g => groupsToRead.add(g));
                });
                return async.eachSeries(Array.from(groupsToRead),
                    (groupId, cb) => this._committedOf(groupId, partitions,
                        (err, offsets) => {
                            if (err) {
                                return cb(err);
                            }
                            sourceOffsets[groupId] = offsets;
                            return cb();
                        }), next);
            },
            next => {
                const perDestination = {};
                servedDestinations.forEach(destinationId => {
                    const named = plan.sourceOf(destinationId);
                    const sources = named || plan.allSourcesOf(doc);
                    const offsets = {};
                    const origin = {};
                    partitions.forEach(partition => {
                        let lowest;
                        sources.forEach(groupId => {
                            const committed =
                                sourceOffsets[groupId][partition];
                            if (committed !== null &&
                                (lowest === undefined || committed < lowest)) {
                                lowest = committed;
                            }
                        });
                        if (lowest === undefined) {
                            offsets[partition] = lows[partition];
                            origin[partition] = 'low watermark, no committed ' +
                                `offset in ${sources.join(', ')}`;
                        } else {
                            offsets[partition] = lowest;
                            origin[partition] = `${plan.sourceLabel} ` +
                                `${sources.length === 1 ? sources[0] :
                                    `lowest of ${sources.join(', ')}`}`;
                        }
                    });
                    perDestination[destinationId] = { offsets, origin, named };
                    // a watermark only where a source really committed:
                    // a destination with no history must get everything
                    const committedOnly = {};
                    partitions.forEach(partition => {
                        if (!origin[partition].startsWith('low watermark')) {
                            committedOnly[String(partition)] = offsets[partition];
                        }
                    });
                    if (Object.keys(committedOnly).length > 0) {
                        result.watermarks[destinationId] = committedOnly;
                    }
                });
                result.destinations = perDestination;
                result.groups = doc.workgroups.map(wg => {
                    const groupId = buildGroupId(this._deliveryPoolConfig.groupId,
                        wg.id, plan.generation);
                    const owned = servedDestinations.filter(destinationId =>
                        workgroupIdForDestination(doc, destinationId) === wg.id);
                    const offsets = {};
                    partitions.forEach(partition => {
                        if (owned.length === 0) {
                            // nothing to deliver: start at the head rather
                            // than read and skip the whole topic
                            offsets[partition] = {
                                offset: highs[partition],
                                source: 'high watermark, the workgroup owns ' +
                                    'no destination',
                            };
                            return;
                        }
                        let lowest;
                        let from;
                        owned.forEach(destinationId => {
                            const o = perDestination[destinationId]
                                .offsets[partition];
                            if (lowest === undefined || o < lowest) {
                                lowest = o;
                                from = destinationId;
                            }
                        });
                        offsets[partition] = {
                            offset: lowest,
                            source: `${from}: ${ 
                                perDestination[from].origin[partition]}`,
                        };
                    });
                    return { groupId, workgroupId: wg.id, destinations: owned,
                        offsets };
                });
                return next();
            },
            next => async.eachSeries(result.groups, (group, cb) =>
                this._seedGroup(group, cb), next),
            next => this._verifySeeded(result.groups, next),
            next => this._writeWatermarks(plan.generation, result.watermarks,
                (err, path) => {
                    if (err) {
                        return next(err);
                    }
                    result.watermarksPath = path;
                    return next();
                }),
        ], err => {
            if (err) {
                return done(err);
            }
            return done(null, result);
        });
    }

    /**
     * Renders a seeding result as a table
     *
     * @param {Object} result - what _seed produced
     * @return {String} text
     */
    static formatResult(result) {
        const lines = [];
        lines.push(`topic ${result.topic}, generation ${result.generation}, ` +
            `partitions ${result.partitions.join(', ')}`);
        lines.push('');
        lines.push(`${'consumer group'.padEnd(52)} ${'partition'.padEnd(9)} ` +
            `${'offset'.padEnd(10)} source`);
        result.groups.forEach(group => {
            result.partitions.forEach(partition => {
                const cell = group.offsets[partition];
                lines.push(`${group.groupId.padEnd(52)} ` +
                    `${String(partition).padEnd(9)} ` +
                    `${String(cell.offset).padEnd(10)} ${cell.source}`);
            });
            lines.push(`${''.padEnd(52)} destinations: ` +
                `${group.destinations.join(', ') || '(none)'}`);
        });
        lines.push('');
        const watermarked = Object.keys(result.watermarks);
        if (result.watermarksPath) {
            lines.push(`watermarks for ${watermarked.length} destination(s) ` +
                `written to ${result.watermarksPath}`);
        } else {
            lines.push('no watermarks written: no source had committed ' +
                'offsets, every matching record will be delivered');
        }
        result.notes.forEach(note => lines.push(note));
        return lines.join('\n');
    }

    _prefixedTopic() {
        return withTopicPrefix(this._notifConfig.topic);
    }

    _assertConfig() {
        if (!this._deliveryPoolConfig || !this._deliveryPoolConfig.groupId) {
            return errors.InvalidArgument.customizeDescription(
                'extensions.notification.deliveryPool.groupId is not ' +
                'configured');
        }
        if (!this._deliveryPoolConfig.workgroups || !this._zkPath) {
            return errors.InvalidArgument.customizeDescription(
                'extensions.notification.deliveryPool.workgroups is not ' +
                'configured: seeding names the groups from the workgroups ' +
                'document in zookeeper');
        }
        if (!this._notifConfig.topic) {
            return errors.InvalidArgument.customizeDescription(
                'extensions.notification.topic is not configured');
        }
        return null;
    }

    /**
     * Destinations a worker on the shared internal topic serves
     *
     * @return {String[]} destination ids
     */
    _servedDestinations() {
        return (this._notifConfig.destinations || [])
            .filter(d => !d.internalTopic ||
                d.internalTopic === this._notifConfig.topic)
            .map(d => d.resource);
    }

    _setupZookeeper(done) {
        if (this._zkClient) {
            return process.nextTick(done);
        }
        this._zkClient = new ZookeeperManager(this._zkConfig.connectionString, {
            autoCreateNamespace: this._zkConfig.autoCreateNamespace,
            retries: this._zkConfig.retries,
        }, this._log);
        this._zkClient.once('error', done);
        this._zkClient.once('ready', () => {
            this._zkClient.removeAllListeners('error');
            done();
        });
        return undefined;
    }

    _readJsonNode(path, done) {
        return this._setupZookeeper(setupErr => {
            if (setupErr) {
                return done(setupErr);
            }
            return this._zkClient.getData(path, undefined, (err, data) => {
                if (err) {
                    if (err.name === 'NO_NODE') {
                        return done(null, null);
                    }
                    return done(err);
                }
                const { error, result } = safeJsonParse(data);
                if (error) {
                    return done(errors.InternalError.customizeDescription(
                        `the document at ${path} is not valid JSON: ` +
                        `${error.message}`));
                }
                return done(null, result);
            });
        });
    }

    _readCurrentDocument(done) {
        return this._readJsonNode(this._zkPath, done);
    }

    /**
     * Keeps a copy of the document being seeded under its generation, so
     * the next layout change can tell which group owned which destination
     *
     * @param {Object} doc - current document
     * @param {Function} done - callback
     * @return {undefined}
     */
    _archiveDocument(doc, done) {
        const path = buildHistoryPath(this._zkPath, doc.generation);
        return this._zkClient.setOrCreate(path,
            Buffer.from(JSON.stringify(doc)), err => {
                if (err) {
                    this._log.warn('could not archive the workgroups document', {
                        method: 'DeliverySeeder._archiveDocument',
                        path,
                        error: err.message,
                    });
                }
                return done();
            });
    }

    /**
     * The document of a previous generation, from --previous-spec, from the
     * history node, or null with a note when neither exists
     *
     * @param {Number} generation - previous generation
     * @param {Function} done - callback: done(err, doc, note)
     * @return {undefined}
     */
    _readPreviousDocument(generation, done) {
        if (this._options.previousSpec) {
            let doc;
            try {
                doc = JSON.parse(fs.readFileSync(this._options.previousSpec,
                    'utf8'));
            } catch (err) {
                return process.nextTick(() => done(
                    errors.InvalidArgument.customizeDescription(
                        'cannot read --previous-spec ' +
                        `${this._options.previousSpec}: ${err.message}`)));
            }
            const { error } = validateWorkgroupsDoc(doc);
            if (error) {
                return process.nextTick(() => done(
                    errors.InvalidArgument.customizeDescription(
                        '--previous-spec is not a valid workgroups document: ' +
                        `${error.message}`)));
            }
            return process.nextTick(() => done(null, doc, null));
        }
        const configError = this._assertConfig();
        if (configError) {
            return process.nextTick(() => done(configError));
        }
        const path = buildHistoryPath(this._zkPath, generation);
        return this._readJsonNode(path, (err, doc) => {
            if (err) {
                return done(err);
            }
            if (doc && !validateWorkgroupsDoc(doc).error &&
                doc.generation === generation) {
                return done(null, doc, null);
            }
            return done(null, null, `no document for generation ${generation} ` +
                `at ${path} and no --previous-spec: each destination is ` +
                'seeded at the lowest offset over every previous group, so ' +
                'the destinations served by the fastest group are delivered ' +
                'again from that offset');
        });
    }

    _setupConsumer(done) {
        if (this._consumer) {
            return process.nextTick(done);
        }
        this._consumer = new KafkaConsumer({
            'metadata.broker.list': this._kafkaConfig.hosts,
            'group.id': `${this._deliveryPoolConfig.groupId}-seed-` +
                `${process.pid}`,
            'enable.auto.commit': false,
            'enable.auto.offset.store': false,
            'allow.auto.create.topics': false,
        }, {});
        this._consumer.on('event.error', err =>
            this._log.error('rdkafka.error', { err }));
        return this._consumer.connect({ timeout: CONNECT_TIMEOUT_MS }, done);
    }

    _getPartitions(done) {
        const topic = this._prefixedTopic();
        return this._setupConsumer(setupErr => {
            if (setupErr) {
                return done(setupErr);
            }
            return this._consumer.getMetadata({ topic, timeout: this._timeout },
                (err, metadata) => {
                    if (err) {
                        return done(errors.InternalError.customizeDescription(
                            `error getting metadata for topic ${topic}: ` +
                            `${err.message || err}`));
                    }
                    const topicMd = (metadata.topics || [])
                        .find(t => t.name === topic);
                    if (!topicMd || topicMd.partitions.length === 0) {
                        return done(errors.InternalError.customizeDescription(
                            `topic ${topic} has no partitions`));
                    }
                    return done(null, topicMd.partitions.map(p => p.id)
                        .sort((a, b) => a - b));
                });
        });
    }

    /**
     * Low and high watermarks of every partition
     *
     * @param {Number[]} partitions - partitions
     * @param {Function} done - callback: done(err, { lows, highs })
     * @return {undefined}
     */
    _queryWatermarks(partitions, done) {
        const topic = this._prefixedTopic();
        const lows = {};
        const highs = {};
        return async.eachSeries(partitions, (partition, next) =>
            this._consumer.queryWatermarkOffsets(topic, partition,
                this._timeout, (err, offsets) => {
                    if (err) {
                        return next(errors.InternalError.customizeDescription(
                            `error querying the watermarks of ${topic} ` +
                            `partition ${partition}: ${err.message || err}`));
                    }
                    lows[partition] = offsets.lowOffset;
                    highs[partition] = offsets.highOffset;
                    return next();
                }), err => done(err, { lows, highs }));
    }

    _defaultGroupClient(groupId) {
        return new KafkaConsumer({
            'metadata.broker.list': this._kafkaConfig.hosts,
            'group.id': groupId,
            'enable.auto.commit': false,
            'enable.auto.offset.store': false,
        }, {});
    }

    _withGroupConsumer(groupId, fn, done) {
        const consumer = this._groupClientFactory(groupId);
        return consumer.connect({ timeout: CONNECT_TIMEOUT_MS }, connectErr => {
            if (connectErr) {
                return done(connectErr);
            }
            return fn(consumer, (err, result) =>
                consumer.disconnect(() => done(err, result)));
        });
    }

    /**
     * Committed offsets of a group, null where it has none
     *
     * @param {String} groupId - consumer group id
     * @param {Number[]} partitions - partitions
     * @param {Function} done - callback: done(err, { partition: offset|null })
     * @return {undefined}
     */
    _committedOf(groupId, partitions, done) {
        const topic = this._prefixedTopic();
        const toppars = partitions.map(partition => ({ topic, partition }));
        return this._withGroupConsumer(groupId, (consumer, cb) =>
            consumer.committed(toppars, this._timeout, cb),
        (err, committedToppars) => {
            if (err) {
                return done(errors.InternalError.customizeDescription(
                    `error reading the committed offsets of group ${groupId}: ` +
                    `${err.message || err}`));
            }
            const offsets = {};
            partitions.forEach(partition => { offsets[partition] = null; });
            (committedToppars || []).forEach(tp => {
                offsets[tp.partition] = typeof tp.offset === 'number' &&
                    tp.offset >= 0 ? tp.offset : null;
            });
            return done(null, offsets);
        });
    }

    _seedGroup(group, done) {
        const topic = this._prefixedTopic();
        const toppars = Object.keys(group.offsets).map(partition => ({
            topic,
            partition: Number(partition),
            offset: group.offsets[partition].offset,
        }));
        return this._withGroupConsumer(group.groupId, (consumer, next) => {
            try {
                consumer.assign(toppars.map(tp =>
                    ({ topic: tp.topic, partition: tp.partition })));
                consumer.commitSync(toppars);
            } catch (err) {
                return next(this._commitError(group.groupId, err));
            }
            this._log.info('seeded a consumer group', {
                method: 'DeliverySeeder._seedGroup',
                groupId: group.groupId,
                offsets: toppars.map(tp =>
                    ({ partition: tp.partition, offset: tp.offset })),
            });
            return next();
        }, done);
    }

    _commitError(groupId, err) {
        const code = err && (err.code !== undefined ? err.code : err.errno);
        if (GROUP_HAS_MEMBERS_CODES.indexOf(code) !== -1) {
            return errors.InternalError.customizeDescription(
                `the consumer group ${groupId} already has members, stop the ` +
                `workers of that generation before seeding it: ${err.message}`);
        }
        return errors.InternalError.customizeDescription(
            `error seeding the offsets of consumer group ${groupId}: ` +
            `${err.message}`);
    }

    _verifySeeded(groups, done) {
        return async.eachSeries(groups, (group, next) => {
            const partitions = Object.keys(group.offsets).map(Number);
            return this._committedOf(group.groupId, partitions,
                (err, committed) => {
                    if (err) {
                        return next(err);
                    }
                    const mismatched = partitions.filter(partition =>
                        committed[partition] !== group.offsets[partition].offset);
                    if (mismatched.length > 0) {
                        return next(errors.InternalError.customizeDescription(
                            `consumer group ${group.groupId} did not keep its ` +
                            `seeded offsets on partitions ${mismatched.join(', ')}`));
                    }
                    return next();
                });
        }, done);
    }

    _writeWatermarks(generation, watermarks, done) {
        if (Object.keys(watermarks).length === 0) {
            return process.nextTick(() => done(null, null));
        }
        const path = buildWatermarksPath(this._zkPath, generation);
        return this._zkClient.setOrCreate(path,
            Buffer.from(JSON.stringify(watermarks)), err => {
                if (err) {
                    return done(errors.InternalError.customizeDescription(
                        `error writing the watermarks to ${path}: ` +
                        `${err.message}`));
                }
                this._log.info('wrote the per destination watermarks', {
                    method: 'DeliverySeeder._writeWatermarks',
                    path,
                    destinations: Object.keys(watermarks).length,
                });
                return done(null, path);
            });
    }
}

module.exports = DeliverySeeder;
module.exports.buildHistoryPath = buildHistoryPath;
