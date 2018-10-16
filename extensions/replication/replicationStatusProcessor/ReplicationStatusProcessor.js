'use strict'; // eslint-disable-line

const http = require('http');
const https = require('https');

const Logger = require('werelogs').Logger;
const errors = require('arsenal').errors;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const VaultClientCache = require('../../../lib/clients/VaultClientCache');
const ReplicationTaskScheduler = require('../utils/ReplicationTaskScheduler');
const UpdateReplicationStatus = require('../tasks/UpdateReplicationStatus');
const QueueEntry = require('../../../lib/models/QueueEntry');
const ObjectQueueEntry = require('../utils/ObjectQueueEntry');

/**
* Given that the largest object JSON from S3 is about 1.6 MB and adding some
* padding to it, Backbeat replication topic is currently setup with a config
* max.message.bytes.limit to 5MB. Consumers need to update their fetchMaxBytes
* to get atleast 5MB put in the Kafka topic, adding a little extra bytes of
* padding for approximation.
*/
const CONSUMER_FETCH_MAX_BYTES = 5000020;

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
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {string} zkConfig.connectionString - zookeeper connection string
     *   as "host:port[/chroot]"
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
     * @param {Object} [internalHttpsConfig] - internal HTTPS
     *   configuration object
     * @param {String} [internalHttpsConfig.key] - client private key
     *   in PEM format
     * @param {String} [internalHttpsConfig.cert] - client certificate
     *   in PEM format
     * @param {String} [internalHttpsConfig.ca] - alternate CA bundle
     *   in PEM format
     */
    constructor(zkConfig, sourceConfig, repConfig, internalHttpsConfig) {
        this.zkConfig = zkConfig;
        this.sourceConfig = sourceConfig;
        this.repConfig = repConfig;
        this.internalHttpsConfig = internalHttpsConfig;
        this._consumer = null;

        this.logger =
            new Logger('Backbeat:Replication:ReplicationStatusProcessor');

        // global variables
        if (sourceConfig.transport === 'https') {
            this.sourceHTTPAgent = new https.Agent({
                key: internalHttpsConfig.key,
                cert: internalHttpsConfig.cert,
                ca: internalHttpsConfig.ca,
                keepAlive: true,
            });
        } else {
            this.sourceHTTPAgent = new http.Agent({ keepAlive: true });
        }

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
            if (this.sourceConfig.transport === 'https') {
                // provision HTTPS credentials for local Vault S3 route
                this.vaultclientCache.setHttps(
                    'source:s3', this.internalHttpsConfig.key,
                    this.internalHttpsConfig.cert,
                    this.internalHttpsConfig.ca);
            }
        }
    }

    getStateVars() {
        return {
            sourceConfig: this.sourceConfig,
            repConfig: this.repConfig,
            internalHttpsConfig: this.internalHttpsConfig,
            sourceHTTPAgent: this.sourceHTTPAgent,
            vaultclientCache: this.vaultclientCache,
            logger: this.logger,
        };
    }

    /**
     * Start kafka consumer
     *
     * @return {undefined}
     */
    start() {
        this._consumer = new BackbeatConsumer({
            zookeeper: { connectionString: this.zkConfig.connectionString },
            topic: this.repConfig.replicationStatusTopic,
            groupId: this.repConfig.replicationStatusProcessor.groupId,
            concurrency:
            this.repConfig.replicationStatusProcessor.concurrency,
            queueProcessor: this.processKafkaEntry.bind(this),
            fetchMaxBytes: CONSUMER_FETCH_MAX_BYTES,
        });
        this._consumer.on('error', () => {});
        this._consumer.subscribe();

        this.logger.info('replication status processor is ready to consume ' +
                         'replication status entries');
    }

    /**
     * Stop kafka consumer and commit current offset
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    stop(done) {
        if (!this._consumer) {
            return setImmediate(done);
        }
        return this._consumer.close(done);
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

    /**
     * Bootstrap kafka consumer by periodically sending bootstrap
     * messages and wait until it's receiving newly produced messages
     * in a timely fashion. ONLY USE FOR TESTING PURPOSE.
     *
     * @param {function} cb - callback when consumer is effectively
     * receiving newly produced messages
     * @return {undefined}
     */
    bootstrapKafkaConsumer(cb) {
        this._consumer.bootstrap(cb);
    }
}

module.exports = ReplicationStatusProcessor;
