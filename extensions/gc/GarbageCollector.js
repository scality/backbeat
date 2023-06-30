'use strict'; // eslint-disable-line

const { EventEmitter } = require('events');

const errors = require('arsenal').errors;
const Logger = require('werelogs').Logger;

const BackbeatConsumer = require('../../lib/BackbeatConsumer');
const ActionQueueEntry = require('../../lib/models/ActionQueueEntry');
const ClientManager = require('../../lib/clients/ClientManager');
const GarbageCollectorTask = require('./tasks/GarbageCollectorTask');

const { authTypeAssumeRole } = require('../../lib/constants');

const { AccountIdCache } = require('../utils/AccountIdCache');
const VaultClientWrapper = require('../utils/VaultClientWrapper');
const GC_CLIENT_ID = 'garbage-collector';

/**
 * @class GarbageCollector
 *
 * @classdesc Background task that deletes unused data blobs to
 * reclaim storage space
 */
class GarbageCollector extends EventEmitter {

    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.kafkaConfig - kafka configuration object
     * @param {string} params.kafkaConfig.hosts - list of kafka
     *   brokers as "host:port[,host:port...]"
     * @param {Object} params.s3Config - S3 configuration
     * @param {Object} params.s3Config.host - s3 endpoint host
     * @param {Number} params.s3Config.port - s3 endpoint port
     * @param {Object} params.gcConfig - garbage collector
     * configuration object
     * @param {String} params.gcConfig.topic - garbage collector kafka
     * topic
     * @param {Object} params.gcConfig.auth - garbage collector
     *   authentication object
     * @param {Object} params.gcConfig.consumer - kafka consumer
     * object
     * @param {String} params.gcConfig.consumer.groupId - kafka
     * consumer group id
     * @param {number} [params.gcConfig.consumer.retry.timeoutS] -
     *  retry timeout in secs.
     * @param {number} [params.gcConfig.consumer.retry.maxRetries] -
     *  max retries before giving up
     * @param {Object} [params.gcConfig.consumer.retry.backoff] -
     *  backoff params
     * @param {number} [params.gcConfig.consumer.retry.backoff.min] -
     *  min. backoff in ms.
     * @param {number} [params.gcConfig.consumer.retry.backoff.max] -
     *  max. backoff in ms.
     * @param {number} [params.gcConfig.consumer.retry.backoff.jitter] -
     *  randomness
     * @param {number} [params.gcConfig.consumer.retry.backoff.factor] -
     *  backoff factor
     * @param {Number} [params.gcConfig.consumer.concurrency] - number
     *  of max allowed concurrent operations
     * @param {String} [params.transport='http'] - transport
     */
    constructor(params) {
        super();

        this._kafkaConfig = params.kafkaConfig;
        this._gcConfig = params.gcConfig;
        this._consumer = null;
        this._started = false;
        this._isActive = false;

        this._logger = new Logger('Backbeat:GC');

        this.clientManager = new ClientManager({
            id: 'garbage collector',
            authConfig: this._gcConfig.auth,
            s3Config: params.s3Config,
            transport: params.transport,
        }, this._logger);

        this.vaultClientWrapper = new VaultClientWrapper(
            GC_CLIENT_ID,
            this._gcConfig.vaultAdmin,
            this._gcConfig.auth,
            this._logger,
        );

        this._accountIdCache = new AccountIdCache(this._gcConfig.consumer.concurrency);
    }

    getAccountId(ownerId, log, cb) {
        if (this._gcConfig.auth.type !== authTypeAssumeRole) {
            log.debug('skipping: not assume role auth type');
            return process.nextTick(cb);
        }

        if (this._accountIdCache.isKnown(ownerId)) {
            return process.nextTick(cb, null, this._accountIdCache.get(ownerId));
        }

        return this.vaultClientWrapper.getAccountId(ownerId, (err, accountId) => {
            if (err) {
                if (err.NoSuchEntity) {
                    log.error('canonical id does not exist', { error: err, ownerId });
                    this._accountIdCache.miss(ownerId);
                } else {
                    log.error('could not get account id', { error: err, ownerId });
                }

                return cb(err);
            }

            this._accountIdCache.set(ownerId, accountId);
            this._accountIdCache.expireOldest();

            return cb(null, accountId);
        });
    }

    /**
     * Start kafka consumer. Emits a 'ready' event when
     * consumer is ready.
     *
     * @return {undefined}
     */
    start() {
        let consumerReady = false;
        this._consumer = new BackbeatConsumer({
            kafka: {
                hosts: this._kafkaConfig.hosts,
                site: this._kafkaConfig.site,
            },
            topic: this._gcConfig.topic,
            groupId: this._gcConfig.consumer.groupId,
            concurrency: this._gcConfig.consumer.concurrency,
            queueProcessor: this.processKafkaEntry.bind(this),
        });
        this._consumer.on('error', () => {
            if (!consumerReady) {
                this._logger.error('garbage collector failed to start the ' +
                                   'kafka consumer');
                process.exit(1);
            }
        });
        this._consumer.on('ready', () => {
            consumerReady = true;
            this._consumer.subscribe();
            this._logger.info('garbage collector service successfully started');
            return this.emit('ready');
        });

        this.clientManager.initSTSConfig();
        this.clientManager.initCredentialsManager();

        if (this._gcConfig.auth.type === authTypeAssumeRole) {
            this.vaultClientWrapper.init();
        }
    }

    /**
     * Close the lifecycle consumer
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        this._logger.debug('closing garbage collector consumer');
        this._consumer.close(cb);
    }

    processKafkaEntry(kafkaEntry, done) {
        this._logger.debug('processing kafka entry');

        const actionEntry = ActionQueueEntry.createFromKafkaEntry(kafkaEntry);
        if (actionEntry.error) {
            this._logger.error(
                'malformed kafka entry from garbage collector topic',
                { error: actionEntry.error.message });
            return process.nextTick(() => done(errors.InternalError));
        }
        const task = new GarbageCollectorTask(this);
        return task.processActionEntry(actionEntry, done);
    }

    getStateVars() {
        return {
            gcConfig: this._gcConfig,
            logger: this._logger,
            getS3Client:
                this.clientManager.getS3Client.bind(this.clientManager),
            getBackbeatClient:
                this.clientManager.getBackbeatClient.bind(this.clientManager),
            getBackbeatMetadataProxy:
                this.clientManager.getBackbeatMetadataProxy.bind(this.clientManager),
            getAccountId: this.getAccountId.bind(this),
        };
    }

    isReady() {
        return this._consumer && this._consumer.isReady()
            && this.vaultClientWrapper.tempCredentialsReady();
    }
}

module.exports = GarbageCollector;
