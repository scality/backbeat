'use strict'; // eslint-disable-line

const async = require('async');
const http = require('http');

const Logger = require('werelogs').Logger;
const errors = require('arsenal').errors;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const BackbeatProducer = require('../../../lib/BackbeatProducer');
const VaultClientCache = require('../../../lib/clients/VaultClientCache');
const ReplicationTaskScheduler = require('../utils/ReplicationTaskScheduler');
const UpdateReplicationStatus = require('../tasks/UpdateReplicationStatus');
const QueueEntry = require('../../../lib/models/QueueEntry');
const ObjectQueueEntry = require('../utils/ObjectQueueEntry');

/**
 * @class ReplicationStatusProcessor
 *
 * @classdesc Background task that processes entries from the
 * replication status kafka queue and updates replication status on
 * source objects.
 */
class ReplicationStatusProcessor {

    /**
     * @constructor
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} sourceConfig - source S3 configuration
     * @param {Object} sourceConfig.s3 - s3 endpoint configuration object
     * @param {Object} sourceConfig.auth - authentication info on source
     * @param {Object} repConfig - replication configuration object
     * @param {String} repConfig.replicationStatusTopic - replication
     *   status topic name
     * @param {String} repConfig.replicationStatusProcessor - config object
     *   specific to replication status processor
     * @param {String} repConfig.replicationStatusProcessor.groupId - kafka
     *   consumer group ID
     * @param {String} repConfig.replicationStatusProcessor.retryTimeoutS -
     *   number of seconds before giving up retries of an entry status
     *   update
     * @param {Object} gcConfig - garbage collection config object
     * @param {String} gcConfig.topic - garbage collection kafka topic
     */
    constructor(kafkaConfig, sourceConfig, repConfig, gcConfig) {
        this.kafkaConfig = kafkaConfig;
        this.sourceConfig = sourceConfig;
        this.repConfig = repConfig;
        this.gcConfig = gcConfig;
        this._consumer = null;
        this._gcProducer = null;

        this.logger =
            new Logger('Backbeat:Replication:ReplicationStatusProcessor');

        // global variables
        // TODO: for SSL support, create HTTPS agents instead
        this.sourceHTTPAgent = new http.Agent({ keepAlive: true });

        this._setupVaultclientCache();

        this.taskScheduler = new ReplicationTaskScheduler(
            (ctx, done) => ctx.task.processQueueEntry(ctx.entry, done));
    }

    _setupVaultclientCache() {
        this.vaultclientCache = new VaultClientCache();

        if (this.sourceConfig.auth.type === 'role') {
            const { host, port } = this.sourceConfig.auth.vault;
            this.vaultclientCache
                .setHost('source:s3', host)
                .setPort('source:s3', port);
        }
    }

    getStateVars() {
        return {
            sourceConfig: this.sourceConfig,
            repConfig: this.repConfig,
            sourceHTTPAgent: this.sourceHTTPAgent,
            vaultclientCache: this.vaultclientCache,
            gcProducer: this._gcProducer,
            logger: this.logger,
        };
    }

    /**
     * Start kafka consumer
     *
     * @param {object} [options] - options object (only used for tests
     * for now)
     * @param {function} [cb] - optional callback called when startup
     * is complete
     * @return {undefined}
     */
    start(options, cb) {
        async.parallel([
            done => {
                this._consumer = new BackbeatConsumer({
                    kafka: { hosts: this.kafkaConfig.hosts },
                    topic: this.repConfig.replicationStatusTopic,
                    groupId: this.repConfig.replicationStatusProcessor.groupId,
                    concurrency:
                    this.repConfig.replicationStatusProcessor.concurrency,
                    queueProcessor: this.processKafkaEntry.bind(this),
                    bootstrap: options && options.bootstrap,
                });
                this._consumer.on('error', () => {});
                this._consumer.on('ready', () => {
                    this._consumer.subscribe();
                    done();
                });
            },
            done => {
                this._gcProducer = new BackbeatProducer({
                    kafka: { hosts: this.kafkaConfig.hosts },
                    topic: this.gcConfig.topic,
                    keyedPartitioner: false,
                });
                this._gcProducer.on('error', () => {});
                this._gcProducer.on('ready', () => done());
            },
        ], () => {
            this.logger.info('replication status processor is ready ' +
                             'to consume replication status entries');
            if (cb) {
                cb();
            }
        });
    }

    /**
     * Stop kafka consumer and producer and commit current consumer offset
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    stop(cb) {
        return async.parallel([
            done => {
                if (!this._consumer) {
                    return setImmediate(done);
                }
                return this._consumer.close(done);
            },
            done => {
                if (!this._gcProducer) {
                    return setImmediate(done);
                }
                return this._gcProducer.close(done);
            },
        ], cb);
    }

    /**
     * Proceed with updating the replication status of an object given
     * a kafka replication status queue entry
     *
     * @param {object} kafkaEntry - entry generated by the replication
     *   queue processor
     * @param {string} kafkaEntry.key - kafka entry key
     * @param {string} kafkaEntry.value - kafka entry value
     * @param {function} done - callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, done) {
        const sourceEntry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        if (sourceEntry.error) {
            this.logger.error('error processing source entry',
                              { error: sourceEntry.error });
            return process.nextTick(() => done(errors.InternalError));
        }
        let task;
        if (sourceEntry instanceof ObjectQueueEntry) {
            task = new UpdateReplicationStatus(this);
        }
        if (task) {
            return this.taskScheduler.push({ task, entry: sourceEntry },
                                           sourceEntry.getCanonicalKey(),
                                           done);
        }
        this.logger.warning('skipping unknown source entry',
                            { entry: sourceEntry.getLogInfo() });
        return process.nextTick(done);
    }
}

module.exports = ReplicationStatusProcessor;
