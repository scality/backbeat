'use strict';

const async = require('async');
const fs = require('fs');
const { KafkaConsumer, Producer, CODES } = require('node-rdkafka');
const { errors, jsutil } = require('arsenal');

const ZookeeperManager = require('../../../lib/clients/ZookeeperManager');
const safeJsonParse = require('../../../lib/util/safeJsonParse');
const { withTopicPrefix } = require('../../../lib/util/topic');

const CONNECT_TIMEOUT_MS = 30000;
const DEFAULT_TIMEOUT_MS = 10000;
const PRODUCER_POLL_MS = 100;

// the broker refuses an offset commit carrying an empty member id once the
// group has live members, so a group whose workers are already running
// cannot be pre-seeded. Retrying never helps, the workers have to be stopped.
const GROUP_HAS_MEMBERS_CODES = [
    CODES.ERRORS.ERR_ILLEGAL_GENERATION,
    CODES.ERRORS.ERR_UNKNOWN_MEMBER_ID,
    CODES.ERRORS.ERR_REBALANCE_IN_PROGRESS,
];

/**
 * @class WorkgroupCutover
 *
 * @classdesc Moves the delivery pool from one workgroups generation to the
 * next: it marks every partition of the delivery topic with a barrier
 * record, writes the new document to zookeeper with those barrier offsets in
 * it, and pre-seeds the new generation's consumer groups at exactly those
 * offsets so the new workers start where the old ones are still working.
 *
 * It never writes a previous generation's offsets, and never reads them to
 * seed a new group. The only thing it reads them for is the drain report,
 * which tells the operator when the previous generation may be stopped.
 */
class WorkgroupCutover {
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
     *   topic metadata, for tests
     * @param {Object} [params.producer] - node-rdkafka producer, for tests
     * @param {Function} [params.groupClientFactory] - (groupId) to consumer
     *   bound to that group, for tests
     * @param {Object} [params.membership] - membership module, for tests
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
        this._producer = params.producer || null;
        this._ownsProducer = !params.producer;
        this._groupClientFactory = params.groupClientFactory ||
            (groupId => this._defaultGroupClient(groupId));
        this._membership = params.membership || null;
    }

    /**
     * The membership module, resolved on first use rather than at module
     * load so an injected one is enough to build this class.
     *
     * @return {Object} membership module
     */
    _wg() {
        if (!this._membership) {
            this._membership = require('../utils/workgroups');
        }
        return this._membership;
    }

    /**
     * Prints the workgroups document currently in zookeeper
     *
     * @param {Function} done - callback: done(err, doc), doc null when the
     *   node does not exist
     * @return {undefined}
     */
    show(done) {
        const configError = this._assertConfig();
        if (configError) {
            return process.nextTick(() => done(configError));
        }
        return this._readCurrentDocument(done);
    }

    /**
     * Builds the document a cutover would write and the destination to
     * workgroup map it implies, without touching anything
     *
     * @param {Function} done - callback: done(err, plan)
     * @return {undefined}
     */
    plan(done) {
        const configError = this._assertConfig();
        if (configError) {
            return process.nextTick(() => done(configError));
        }
        return this._readCurrentDocument((err, currentDoc) => {
            if (err) {
                return done(err);
            }
            const { error, doc } = this.buildDocument(currentDoc);
            if (error) {
                return done(error);
            }
            return done(null, {
                doc,
                groupIds: this.groupIdsOf(doc),
                previousGroupIds: this.previousGroupIds(currentDoc),
                destinations: this._destinationMap(doc),
            });
        });
    }

    /**
     * Produces the barriers, writes the new document and pre-seeds the new
     * generation's consumer groups
     *
     * @param {Function} done - callback: done(err, result)
     * @return {undefined}
     */
    cutover(done) {
        const configError = this._assertConfig();
        if (configError) {
            return process.nextTick(() => done(configError));
        }
        let newDoc = null;
        let previousGroupIds = null;
        return async.waterfall([
            next => this._readCurrentDocument(next),
            (currentDoc, next) => {
                const { error, doc } = this.buildDocument(currentDoc);
                if (error) {
                    return next(error);
                }
                newDoc = doc;
                previousGroupIds = this.previousGroupIds(currentDoc);
                this._log.info('cutting over to a new workgroups generation', {
                    method: 'WorkgroupCutover.cutover',
                    generation: doc.generation,
                    workgroups: doc.workgroups.map(wg => wg.id),
                    previousGroupIds,
                });
                return next();
            },
            next => this._getPartitions(next),
            (partitions, next) =>
                this._produceBarriers(partitions, newDoc.generation, next),
            (barriers, next) => {
                newDoc.barriers = barriers;
                return this._writeDocument(newDoc, next);
            },
            next => this._seedGroups(newDoc, err => next(err)),
            next => this._verifySeeded(newDoc, next),
            next => this._drainReport(previousGroupIds, newDoc.barriers, next),
        ], (err, report) => {
            if (err) {
                return done(err);
            }
            return done(null, {
                doc: newDoc,
                groupIds: this.groupIdsOf(newDoc),
                previousGroupIds,
                report,
            });
        });
    }

    /**
     * Pre-seeds the consumer groups of the document already in zookeeper.
     * Committing the same offsets twice is a no-op, so this can be rerun
     * after any partial failure.
     *
     * @param {Function} done - callback: done(err, result)
     * @return {undefined}
     */
    preseed(done) {
        const configError = this._assertConfig();
        if (configError) {
            return process.nextTick(() => done(configError));
        }
        let doc = null;
        return async.waterfall([
            next => this._readCurrentDocument(next),
            (currentDoc, next) => {
                const error = this._assertSeedable(currentDoc);
                if (error) {
                    return next(error);
                }
                doc = currentDoc;
                return next();
            },
            next => this._seedGroups(doc, err => next(err)),
            next => this._verifySeeded(doc, next),
        ], err => {
            if (err) {
                return done(err);
            }
            return done(null, {
                doc,
                groupIds: this.groupIdsOf(doc),
            });
        });
    }

    /**
     * Reports how far the previous generation has drained towards the
     * barriers of the generation now in zookeeper
     *
     * @param {Function} done - callback: done(err, report)
     * @return {undefined}
     */
    verify(done) {
        const configError = this._assertConfig();
        if (configError) {
            return process.nextTick(() => done(configError));
        }
        return this._readCurrentDocument((err, doc) => {
            if (err) {
                return done(err);
            }
            const seedable = this._assertSeedable(doc);
            if (seedable) {
                return done(seedable);
            }
            return this._drainReport(this.previousGroupIdsOfRunning(doc),
                doc.barriers, done);
        });
    }

    /**
     * Releases every client this class opened
     *
     * @param {Function} done - callback
     * @return {undefined}
     */
    close(done) {
        return async.series([
            next => {
                if (this._producer && this._ownsProducer) {
                    return this._producer.disconnect(() => next());
                }
                return process.nextTick(next);
            },
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
     * Builds the document the options describe, at the generation that
     * follows the one in zookeeper.
     *
     * The set of groups this generation replaces is recorded in the document
     * itself, because it cannot be derived once the document it comes from
     * has been overwritten: a cutover that renames or merges workgroups
     * leaves no trace of the ids the previous generation ran under.
     *
     * @param {Object} [currentDoc] - document currently in zookeeper
     * @return {Object} { error, doc }
     */
    buildDocument(currentDoc) {
        const built = this._buildWorkgroups();
        if (built.error) {
            return { error: built.error };
        }
        const currentGeneration = currentDoc ? currentDoc.generation : 0;
        const generation = this._options.generation !== undefined ?
            Number(this._options.generation) : currentGeneration + 1;
        if (!Number.isInteger(generation) || generation < 1) {
            return { error: errors.InternalError.customizeDescription(
                '--generation must be an integer above 0, got ' +
                `${this._options.generation}`) };
        }
        if (generation !== currentGeneration + 1 && !this._options.force) {
            return { error: errors.InternalError.customizeDescription(
                `generation ${generation} does not follow generation ` +
                `${currentGeneration}, the one in zookeeper: rerun with ` +
                '--force to write it anyway') };
        }
        const { error, value } = this._wg().validateWorkgroupsDoc({
            configVersion: this._wg().CONFIG_VERSION,
            generation,
            topic: this._deliveryPoolConfig.topic,
            updatedAt: new Date().toISOString(),
            workgroups: built.workgroups,
            previousGroups: this.previousGroupIds(currentDoc),
        });
        if (error) {
            return { error: errors.InternalError.customizeDescription(
                `the workgroups document is invalid: ${error.message}`) };
        }
        return { error: null, doc: value };
    }

    /**
     * Consumer group ids of a document's workgroups
     *
     * @param {Object} doc - workgroups document
     * @return {String[]} group ids
     */
    groupIdsOf(doc) {
        const base = this._deliveryPoolConfig.groupId;
        return doc.workgroups.map(wg =>
            this._wg().buildGroupId(base, wg.id, doc.generation));
    }

    /**
     * Group ids of the generation a cutover is replacing. Without a document
     * in zookeeper that is the single pool, which is the group the current
     * workers are in.
     *
     * @param {Object} [currentDoc] - document currently in zookeeper
     * @return {String[]} group ids
     */
    previousGroupIds(currentDoc) {
        const fromGroup = this._options.fromGroup;
        if (fromGroup && fromGroup.length > 0) {
            return fromGroup;
        }
        if (!currentDoc) {
            return [this._deliveryPoolConfig.groupId];
        }
        return this.groupIdsOf(currentDoc);
    }

    /**
     * Group ids of the generation before the one now in zookeeper.
     *
     * The document records the set the cutover replaced, so that is the
     * authority. Deriving it from the workgroups the new document lists, one
     * generation back, is only a fallback for a document written before the
     * tool recorded the set: that derivation misses every group whose
     * workgroup was renamed, merged or dropped by the same cutover.
     * --from-group overrides both.
     *
     * @param {Object} doc - document currently in zookeeper
     * @return {String[]} group ids
     */
    previousGroupIdsOfRunning(doc) {
        const fromGroup = this._options.fromGroup;
        if (fromGroup && fromGroup.length > 0) {
            return fromGroup;
        }
        if (doc.previousGroups && doc.previousGroups.length > 0) {
            return doc.previousGroups;
        }
        const base = this._deliveryPoolConfig.groupId;
        this._log.warn('the workgroups document does not record the groups ' +
            'it replaced, falling back to deriving them from the workgroups ' +
            'it lists: a workgroup renamed by that cutover will be missing ' +
            'from the drain report, name it with --from-group', {
            method: 'WorkgroupCutover.previousGroupIdsOfRunning',
            generation: doc.generation,
        });
        if (doc.generation <= 1) {
            return [base];
        }
        return doc.workgroups.map(wg =>
            this._wg().buildGroupId(base, wg.id, doc.generation - 1));
    }

    /**
     * Partitions to commit, at exactly the offset of their barrier record.
     * A committed offset is the next offset to fetch, so the barrier is the
     * first record every worker of the new generation sees.
     *
     * @param {Object} barriers - barrier offsets by partition
     * @return {Object[]} toppars carrying an offset
     */
    buildPreseedToppars(barriers) {
        const topic = this._prefixedTopic();
        return WorkgroupCutover.partitionsOf(barriers).map(partition => ({
            topic,
            partition,
            offset: barriers[partition],
        }));
    }

    /**
     * Partition numbers of a barriers object, in order
     *
     * @param {Object} barriers - barrier offsets by partition
     * @return {Number[]} partition ids
     */
    static partitionsOf(barriers) {
        return Object.keys(barriers).map(Number).sort((a, b) => a - b);
    }

    /**
     * Turns barriers and committed offsets into the rows of a drain report.
     *
     * A group with no committed offset on a partition has delivered nothing
     * of it, so everything up to the barrier is still its responsibility.
     *
     * Each row carries both directions around the barrier. `remaining` is
     * what the previous generation still owes and is the only thing the exit
     * code keys off. `overshoot` is what it has already consumed past the
     * barrier: the new generation is seeded at the barrier and delivers those
     * same records again, so each one is a duplicate, and the count keeps
     * climbing for as long as the previous generation is left running.
     *
     * @param {Object} params - report params
     * @param {Object} params.barriers - barrier offsets by partition
     * @param {Object} params.committedByGroup - group id to committed
     *   offsets by partition
     * @return {Object} { rows, drained, overshootByGroup, overshoot }
     */
    static buildDrainReport(params) {
        const { barriers, committedByGroup } = params;
        const partitions = WorkgroupCutover.partitionsOf(barriers);
        const rows = [];
        const overshootByGroup = {};
        Object.keys(committedByGroup).forEach(groupId => {
            const committed = committedByGroup[groupId];
            overshootByGroup[groupId] = 0;
            partitions.forEach(partition => {
                const barrier = barriers[partition];
                const raw = committed[partition];
                const seen = typeof raw === 'number' && raw >= 0 ? raw : 0;
                const overshoot = Math.max(0, seen - barrier);
                overshootByGroup[groupId] += overshoot;
                rows.push({
                    groupId,
                    partition,
                    barrier,
                    committed: typeof raw === 'number' ? raw : -1,
                    remaining: Math.max(0, barrier - seen),
                    overshoot,
                });
            });
        });
        return {
            rows,
            drained: rows.every(row => row.remaining === 0),
            overshootByGroup,
            overshoot: Object.keys(overshootByGroup).reduce(
                (total, groupId) => total + overshootByGroup[groupId], 0),
        };
    }

    /**
     * Renders a drain report as a padded table, followed by what the
     * overshoot column adds up to for each group
     *
     * @param {Object} report - report from buildDrainReport
     * @return {String} printable table
     */
    static formatDrainReport(report) {
        const header = ['group', 'partition', 'barrier', 'committed',
            'remaining', 'overshoot'];
        const rows = report.rows.map(row => [
            row.groupId,
            String(row.partition),
            String(row.barrier),
            String(row.committed),
            String(row.remaining),
            String(row.overshoot),
        ]);
        const widths = header.map((column, index) => Math.max(column.length,
            ...rows.map(row => row[index].length)));
        const table = [header].concat(rows)
            .map(row => row
                .map((cell, index) => cell.padEnd(widths[index]))
                .join('  ')
                .trimEnd())
            .join('\n');
        const totals = Object.keys(report.overshootByGroup).map(groupId =>
            `  ${groupId.padEnd(widths[0])}  ` +
            `${report.overshootByGroup[groupId]}`);
        return [
            table,
            '',
            'records this group consumed past its barrier, which the new ' +
                'generation',
            'delivers a second time: each one is a duplicate, and the count ' +
                'climbs',
            'for as long as the previous generation keeps running',
        ].concat(totals).join('\n');
    }

    /**
     * Parses a --workgroup <id>:<remainder,...> option
     *
     * @param {String} value - option value
     * @param {Number} modulo - modulo shared by every hashmod rule
     * @return {Object} { error, workgroup }
     */
    static parseHashmodOption(value, modulo) {
        const parsed = WorkgroupCutover.parseListOption(value);
        if (parsed.error) {
            return { error: errors.InternalError.customizeDescription(
                `expected --workgroup <id>:<remainder,...>, got ${value}`) };
        }
        const remainders = parsed.values.map(Number);
        if (remainders.some(remainder => !Number.isInteger(remainder))) {
            return { error: errors.InternalError.customizeDescription(
                `--workgroup remainders must be integers, got ${value}`) };
        }
        return {
            error: null,
            workgroup: {
                id: parsed.id,
                rule: { type: 'hashmod', modulo, remainders },
            },
        };
    }

    /**
     * Parses a --static <id>:<destinationId,...> option
     *
     * @param {String} value - option value
     * @return {Object} { error, workgroup }
     */
    static parseStaticOption(value) {
        const parsed = WorkgroupCutover.parseListOption(value);
        if (parsed.error) {
            return { error: errors.InternalError.customizeDescription(
                `expected --static <id>:<destinationId,...>, got ${value}`) };
        }
        return {
            error: null,
            workgroup: {
                id: parsed.id,
                rule: { type: 'static', destinationIds: parsed.values },
            },
        };
    }

    /**
     * Splits an <id>:<a,b,c> option value
     *
     * @param {String} value - option value
     * @return {Object} { error, id, values }
     */
    static parseListOption(value) {
        const separator = String(value).indexOf(':');
        if (separator === -1) {
            return { error: true };
        }
        const id = String(value).slice(0, separator).trim();
        const values = String(value).slice(separator + 1).split(',')
            .map(part => part.trim())
            .filter(part => part !== '');
        if (id === '' || values.length === 0) {
            return { error: true };
        }
        return { error: null, id, values };
    }

    _prefixedTopic() {
        return withTopicPrefix(this._deliveryPoolConfig.topic);
    }

    /**
     * Checks the configuration the tool needs is complete
     *
     * @return {Object|null} error, or null when the config is usable
     */
    _assertConfig() {
        const deliveryPool = this._deliveryPoolConfig;
        if (!deliveryPool || !deliveryPool.enabled) {
            return errors.InternalError.customizeDescription(
                'extensions.notification.deliveryPool.enabled must be set');
        }
        const missing = ['topic', 'groupId']
            .filter(key => !deliveryPool[key]);
        if (missing.length > 0) {
            return errors.InternalError.customizeDescription(
                'missing extensions.notification.deliveryPool.' +
                `${missing.join(', deliveryPool.')}`);
        }
        if (!deliveryPool.workgroups || !this._zkPath) {
            return errors.InternalError.customizeDescription(
                'missing extensions.notification.deliveryPool.workgroups, ' +
                'this deployment does not run workgroups');
        }
        return null;
    }

    /**
     * Checks a document read from zookeeper can have its groups seeded
     *
     * @param {Object} [doc] - document currently in zookeeper
     * @return {Object|null} error, or null
     */
    _assertSeedable(doc) {
        if (!doc) {
            return errors.InternalError.customizeDescription(
                `no workgroups document at ${this._zkPath}, run cutover first`);
        }
        const { error } = this._wg().validateWorkgroupsDoc(doc);
        if (error) {
            return errors.InternalError.customizeDescription(
                'the workgroups document in zookeeper is invalid: ' +
                `${error.message}`);
        }
        if (!doc.barriers || Object.keys(doc.barriers).length === 0) {
            return errors.InternalError.customizeDescription(
                `the workgroups document at generation ${doc.generation} ` +
                'carries no barriers, so there is no offset to seed from');
        }
        return null;
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

    /**
     * Reads the workgroups document, which may not exist yet
     *
     * @param {Function} done - callback: done(err, doc)
     * @return {undefined}
     */
    _readCurrentDocument(done) {
        return this._setupZookeeper(setupErr => {
            if (setupErr) {
                return done(setupErr);
            }
            return this._zkClient.getData(this._zkPath, undefined,
                (err, data) => {
                    if (err) {
                        if (err.name === 'NO_NODE') {
                            return done(null, null);
                        }
                        return done(err);
                    }
                    const { error, result } = safeJsonParse(data);
                    if (error) {
                        return done(errors.InternalError.customizeDescription(
                            `the workgroups document at ${this._zkPath} is ` +
                            `not valid JSON: ${error.message}`));
                    }
                    return done(null, result);
                });
        });
    }

    /**
     * Writes the new document. This is the commit point of a cutover, so it
     * is read back and checked before anything else happens.
     *
     * @param {Object} doc - document to write
     * @param {Function} done - callback
     * @return {undefined}
     */
    _writeDocument(doc, done) {
        const data = Buffer.from(JSON.stringify(doc));
        return this._zkClient.setOrCreate(this._zkPath, data, writeErr => {
            if (writeErr) {
                return done(writeErr);
            }
            return this._zkClient.getData(this._zkPath, undefined,
                (readErr, readBack) => {
                    if (readErr) {
                        return done(readErr);
                    }
                    const { error, result } = safeJsonParse(readBack);
                    if (error) {
                        return done(errors.InternalError.customizeDescription(
                            'the workgroups document read back after the ' +
                            `write is not valid JSON: ${error.message}`));
                    }
                    if (result.generation !== doc.generation) {
                        return done(errors.InternalError.customizeDescription(
                            `wrote generation ${doc.generation} but read ` +
                            `back generation ${result.generation}`));
                    }
                    const mismatched = WorkgroupCutover
                        .partitionsOf(doc.barriers)
                        .filter(partition => !result.barriers ||
                            result.barriers[partition] !==
                                doc.barriers[partition]);
                    if (mismatched.length > 0) {
                        return done(errors.InternalError.customizeDescription(
                            'the barriers read back differ from the ones ' +
                            `written on partitions ${mismatched.join(', ')}`));
                    }
                    this._log.info('wrote the new workgroups document', {
                        method: 'WorkgroupCutover._writeDocument',
                        zkPath: this._zkPath,
                        generation: doc.generation,
                    });
                    return done();
                });
        });
    }

    _setupConsumer(done) {
        if (this._consumer) {
            return process.nextTick(done);
        }
        this._consumer = new KafkaConsumer({
            'metadata.broker.list': this._kafkaConfig.hosts,
            'group.id': `${this._deliveryPoolConfig.groupId}-cutover-` +
                `${process.pid}`,
            'enable.auto.commit': false,
            'enable.auto.offset.store': false,
            'allow.auto.create.topics': false,
        }, {});
        this._consumer.on('event.error', err =>
            this._log.error('rdkafka.error', { err }));
        return this._consumer.connect({ timeout: CONNECT_TIMEOUT_MS }, done);
    }

    /**
     * Lists the partitions of the delivery topic
     *
     * @param {Function} done - callback: done(err, partitions)
     * @return {undefined}
     */
    _getPartitions(done) {
        const topic = this._prefixedTopic();
        return this._setupConsumer(setupErr => {
            if (setupErr) {
                return done(setupErr);
            }
            return this._consumer.getMetadata({
                topic,
                timeout: this._timeout,
            }, (err, metadata) => {
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
                return done(null, topicMd.partitions.map(p => p.id));
            });
        });
    }

    _setupProducer(done) {
        if (this._producer) {
            return process.nextTick(done);
        }
        // BackbeatProducer cannot write these records: it honours an explicit
        // partition only when the key is 'canary', and its delivery report
        // handler dereferences report.opaque.cbOnce unconditionally, so a
        // record produced on its client throws inside that handler
        this._producer = new Producer({
            'metadata.broker.list': this._kafkaConfig.hosts,
            'dr_cb': true,
        }, {});
        this._producer.on('event.error', err =>
            this._log.error('rdkafka.error', { err }));
        return this._producer.connect({ timeout: CONNECT_TIMEOUT_MS }, err => {
            if (err) {
                return done(err);
            }
            this._producer.setPollInterval(PRODUCER_POLL_MS);
            return done();
        });
    }

    /**
     * Writes one barrier record per partition and collects the offset each
     * one landed at, from its delivery report
     *
     * @param {Number[]} partitions - partition ids
     * @param {Number} generation - generation the barriers belong to
     * @param {Function} done - callback: done(err, barriers)
     * @return {undefined}
     */
    _produceBarriers(partitions, generation, done) {
        const doneOnce = jsutil.once(done);
        const topic = this._prefixedTopic();
        const { BARRIER_KEY, buildBarrierRecord } = this._wg();
        const barriers = {};
        let pending = partitions.length;
        return this._setupProducer(setupErr => {
            if (setupErr) {
                return doneOnce(setupErr);
            }
            const timer = setTimeout(() => doneOnce(
                errors.InternalError.customizeDescription(
                    'timed out waiting for the barrier delivery reports on ' +
                    `topic ${topic}`)), this._timeout);
            this._producer.on('delivery-report', (err, report) => {
                if (err) {
                    clearTimeout(timer);
                    return doneOnce(errors.InternalError.customizeDescription(
                        `error producing a barrier record: ${err.message}`));
                }
                if (typeof report.offset !== 'number' || report.offset < 0) {
                    clearTimeout(timer);
                    return doneOnce(errors.InternalError.customizeDescription(
                        `the barrier of partition ${report.partition} was ` +
                        `acknowledged at offset ${report.offset}`));
                }
                barriers[report.partition] = report.offset;
                pending -= 1;
                if (pending > 0) {
                    return undefined;
                }
                clearTimeout(timer);
                this._log.info('produced the cutover barriers', {
                    method: 'WorkgroupCutover._produceBarriers',
                    topic,
                    generation,
                    barriers,
                });
                return doneOnce(null, barriers);
            });
            try {
                partitions.forEach(partition => this._producer.produce(topic,
                    partition,
                    Buffer.from(buildBarrierRecord({ generation, partition })),
                    BARRIER_KEY, Date.now()));
            } catch (err) {
                clearTimeout(timer);
                return doneOnce(errors.InternalError.customizeDescription(
                    `error producing the barrier records: ${err.message}`));
            }
            return undefined;
        });
    }

    _defaultGroupClient(groupId) {
        return new KafkaConsumer({
            'metadata.broker.list': this._kafkaConfig.hosts,
            'group.id': groupId,
            'enable.auto.commit': false,
            'enable.auto.offset.store': false,
        }, {});
    }

    /**
     * Runs fn against a throwaway consumer bound to a group id. committed()
     * and commitSync() are scoped to the group.id of the client they are
     * called on, so this is the only way to read or seed another group.
     *
     * @param {String} groupId - consumer group id
     * @param {Function} fn - fn(consumer, cb)
     * @param {Function} done - callback: done(err, result)
     * @return {undefined}
     */
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
     * Commits the barrier offsets into every group of the new generation
     *
     * @param {Object} doc - document holding the barriers
     * @param {Function} done - callback: done(err, groupIds)
     * @return {undefined}
     */
    _seedGroups(doc, done) {
        const toppars = this.buildPreseedToppars(doc.barriers);
        const groupIds = this.groupIdsOf(doc);
        return async.eachSeries(groupIds, (groupId, next) =>
            this._seedGroup(groupId, toppars, next), err => {
            if (err) {
                return done(err);
            }
            return done(null, groupIds);
        });
    }

    _seedGroup(groupId, toppars, done) {
        return this._withGroupConsumer(groupId, (consumer, next) => {
            try {
                consumer.assign(toppars.map(tp =>
                    ({ topic: tp.topic, partition: tp.partition })));
                consumer.commitSync(toppars);
            } catch (err) {
                // commitSync throws synchronously rather than calling back
                return next(this._commitError(groupId, err));
            }
            this._log.info('seeded a consumer group', {
                method: 'WorkgroupCutover._seedGroup',
                groupId,
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
                'workers of that generation before pre-seeding it: ' +
                `${err.message}`);
        }
        return errors.InternalError.customizeDescription(
            `error seeding the offsets of consumer group ${groupId}: ` +
            `${err.message}`);
    }

    /**
     * Checks every new group committed exactly its barrier offsets
     *
     * @param {Object} doc - document holding the barriers
     * @param {Function} done - callback
     * @return {undefined}
     */
    _verifySeeded(doc, done) {
        const toppars = this._partitionToppars(doc.barriers);
        return async.eachSeries(this.groupIdsOf(doc), (groupId, next) =>
            this._withGroupConsumer(groupId, (consumer, cb) =>
                consumer.committed(toppars, this._timeout, cb),
            (err, committedToppars) => {
                if (err) {
                    return next(err);
                }
                const offsets = {};
                (committedToppars || []).forEach(tp => {
                    offsets[tp.partition] = tp.offset;
                });
                const mismatched = WorkgroupCutover.partitionsOf(doc.barriers)
                    .filter(partition =>
                        offsets[partition] !== doc.barriers[partition]);
                if (mismatched.length > 0) {
                    return next(errors.InternalError.customizeDescription(
                        `consumer group ${groupId} is not seeded at its ` +
                        `barriers on partitions ${mismatched.join(', ')}`));
                }
                return next();
            }), done);
    }

    _partitionToppars(barriers) {
        const topic = this._prefixedTopic();
        return WorkgroupCutover.partitionsOf(barriers).map(partition =>
            ({ topic, partition }));
    }

    /**
     * Reads how far each previous group has committed towards the barriers.
     * Those groups are only ever read, never assigned and never committed,
     * so the previous generation keeps its offsets and a rollback stays
     * possible.
     *
     * @param {String[]} groupIds - previous generation group ids
     * @param {Object} barriers - barrier offsets by partition
     * @param {Function} done - callback: done(err, report)
     * @return {undefined}
     */
    _drainReport(groupIds, barriers, done) {
        const toppars = this._partitionToppars(barriers);
        const committedByGroup = {};
        return async.eachSeries(groupIds, (groupId, next) =>
            this._withGroupConsumer(groupId, (consumer, cb) =>
                consumer.committed(toppars, this._timeout, cb),
            (err, committedToppars) => {
                if (err) {
                    return next(err);
                }
                const offsets = {};
                (committedToppars || []).forEach(tp => {
                    offsets[tp.partition] = tp.offset;
                });
                committedByGroup[groupId] = offsets;
                return next();
            }), err => {
            if (err) {
                return done(err);
            }
            return done(null, WorkgroupCutover.buildDrainReport({
                barriers,
                committedByGroup,
            }));
        });
    }

    _buildWorkgroups() {
        const options = this._options;
        if (options.spec) {
            return this._readSpec(options.spec);
        }
        const hashmod = options.workgroup || [];
        const statics = options.static || [];
        if (hashmod.length === 0 && statics.length === 0) {
            return { error: errors.InternalError.customizeDescription(
                'no workgroup given: use --workgroup, --static or --spec') };
        }
        if (hashmod.length > 0 && options.modulo === undefined) {
            return { error: errors.InternalError.customizeDescription(
                '--modulo is required alongside --workgroup') };
        }
        const modulo = Number(options.modulo);
        const parsed = hashmod
            .map(value => WorkgroupCutover.parseHashmodOption(value, modulo))
            .concat(statics.map(value =>
                WorkgroupCutover.parseStaticOption(value)));
        const failed = parsed.find(entry => entry.error);
        if (failed) {
            return { error: failed.error };
        }
        return {
            error: null,
            workgroups: parsed.map(entry => entry.workgroup),
        };
    }

    _readSpec(specPath) {
        let raw;
        try {
            raw = fs.readFileSync(specPath, 'utf8');
        } catch (err) {
            return { error: errors.InternalError.customizeDescription(
                `could not read ${specPath}: ${err.message}`) };
        }
        const { error, result } = safeJsonParse(raw);
        if (error) {
            return { error: errors.InternalError.customizeDescription(
                `${specPath} is not valid JSON: ${error.message}`) };
        }
        const workgroups = Array.isArray(result) ? result :
            result && result.workgroups;
        if (!Array.isArray(workgroups)) {
            return { error: errors.InternalError.customizeDescription(
                `${specPath} must hold the workgroups array, either on its ` +
                'own or under a workgroups key') };
        }
        return { error: null, workgroups };
    }

    _destinationMap(doc) {
        const destinations = this._notifConfig.destinations || [];
        return destinations.map(destination => ({
            destinationId: destination.resource,
            workgroupId: this._wg()
                .workgroupIdForDestination(doc, destination.resource),
        }));
    }
}

module.exports = WorkgroupCutover;
