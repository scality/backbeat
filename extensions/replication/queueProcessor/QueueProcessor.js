'use strict'; // eslint-disable-line

const http = require('http');

const Logger = require('werelogs').Logger;

const errors = require('arsenal').errors;
const RoundRobin = require('arsenal').network.RoundRobin;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const VaultClientCache = require('../../../lib/clients/VaultClientCache');
const QueueEntry = require('../utils/QueueEntry');
const ReplicationTaskScheduler = require('../utils/ReplicationTaskScheduler');
const ReplicateObject = require('../tasks/ReplicateObject');
const MultipleBackendTask = require('../tasks/MultipleBackendTask');
const EchoBucket = require('../tasks/EchoBucket');

const ObjectQueueEntry = require('../utils/ObjectQueueEntry');
const BucketQueueEntry = require('../utils/BucketQueueEntry');

const { proxyVaultPath, proxyIAMPath } = require('../constants');

/**
* Given that the largest object JSON from S3 is about 1.6 MB and adding some
* padding to it, Backbeat replication topic is currently setup with a config
* max.message.bytes.limit to 5MB. Consumers need to update their fetchMaxBytes
* to get atleast 5MB put in the Kafka topic, adding a little extra bytes of
* padding for approximation.
*/
const CONSUMER_FETCH_MAX_BYTES = 5000020;

class QueueProcessor {

    /**
     * Create a queue processor object to activate Cross-Region
     * Replication from a kafka topic dedicated to store replication
     * entries to a target S3 endpoint.
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {string} zkConfig.connectionString - zookeeper connection string
     *   as "host:port[/chroot]"
     * @param {Object} sourceConfig - source S3 configuration
     * @param {Object} sourceConfig.s3 - s3 endpoint configuration object
     * @param {Object} sourceConfig.auth - authentication info on source
     * @param {Object} destConfig - target S3 configuration
     * @param {Object} destConfig.auth - authentication info on target
     * @param {Object} repConfig - replication configuration object
     * @param {String} repConfig.topic - replication topic name
     * @param {String} repConfig.queueProcessor - config object
     *   specific to queue processor
     * @param {String} repConfig.queueProcessor.groupId - kafka
     *   consumer group ID
     * @param {String} repConfig.queueProcessor.retryTimeoutS -
     *   number of seconds before giving up retries of an entry
     *   replication
     */
    constructor(zkConfig, sourceConfig, destConfig, repConfig) {
        this.zkConfig = zkConfig;
        this.sourceConfig = sourceConfig;
        this.destConfig = destConfig;
        this.repConfig = repConfig;
        this.destHosts = null;
        this.sourceAdminVaultConfigured = false;
        this.destAdminVaultConfigured = false;

        this.echoMode = false;

        this.logger = new Logger('Backbeat:Replication:QueueProcessor');

        // global variables
        // TODO: for SSL support, create HTTPS agents instead
        this.sourceHTTPAgent = new http.Agent({ keepAlive: true });
        this.destHTTPAgent = new http.Agent({ keepAlive: true });

        this._setupVaultclientCache();

        // FIXME support multiple scality destination sites
        if (Array.isArray(destConfig.bootstrapList)) {
            destConfig.bootstrapList.forEach(dest => {
                if (Array.isArray(dest.servers)) {
                    this.destHosts =
                        new RoundRobin(dest.servers, { defaultPort: 80 });
                    if (dest.echo) {
                        this._setupEcho();
                    }
                }
            });
        }

        this.taskScheduler = new ReplicationTaskScheduler(
            (ctx, done) => ctx.task.processQueueEntry(ctx.entry, done));
    }

    _setupVaultclientCache() {
        this.vaultclientCache = new VaultClientCache();

        if (this.sourceConfig.auth.type === 'role') {
            const { host, port, adminPort, adminCredentials }
                      = this.sourceConfig.auth.vault;
            this.vaultclientCache
                .setHost('source:s3', host)
                .setPort('source:s3', port);
            if (adminCredentials) {
                this.vaultclientCache
                    .setHost('source:admin', host)
                    .setPort('source:admin', adminPort)
                    .loadAdminCredentials('source:admin',
                                          adminCredentials.accessKey,
                                          adminCredentials.secretKey);
                this.sourceAdminVaultConfigured = true;
            }
        }
        if (this.destConfig.auth.type === 'role') {
            if (this.destConfig.auth.vault) {
                const { host, port, adminPort, adminCredentials }
                          = this.destConfig.auth.vault;
                if (host) {
                    this.vaultclientCache.setHost('dest:s3', host);
                }
                if (port) {
                    this.vaultclientCache.setPort('dest:s3', port);
                }
                if (adminCredentials) {
                    if (host) {
                        this.vaultclientCache.setHost('dest:admin', host);
                    }
                    if (adminPort) {
                        this.vaultclientCache.setPort('dest:admin', adminPort);
                    } else {
                        // if dest vault admin port not configured, go
                        // through nginx proxy
                        this.vaultclientCache.setProxyPath('dest:admin',
                                                           proxyIAMPath);
                    }
                    this.vaultclientCache.loadAdminCredentials(
                        'dest:admin',
                        adminCredentials.accessKey,
                        adminCredentials.secretKey);
                    this.destAdminVaultConfigured = true;
                }
            }
            if (!this.destConfig.auth.vault ||
                !this.destConfig.auth.vault.port) {
                // if dest vault port not configured, go through nginx
                // proxy
                this.vaultclientCache.setProxyPath('dest:s3',
                                                   proxyVaultPath);
            }
        }
    }

    _setupEcho() {
        if (!this.sourceAdminVaultConfigured) {
            throw new Error('echo mode not properly configured: missing ' +
                            'credentials for source Vault admin client');
        }
        if (!this.destAdminVaultConfigured) {
            throw new Error('echo mode not properly configured: missing ' +
                            'credentials for destination Vault ' +
                            'admin client');
        }
        if (process.env.BACKBEAT_ECHO_TEST_MODE === '1') {
            this.logger.info('starting in echo mode',
                             { method: 'QueueProcessor.constructor',
                               testMode: true });
        } else {
            this.logger.info('starting in echo mode',
                             { method: 'QueueProcessor.constructor' });
        }
        this.echoMode = true;
        this.accountCredsCache = {};
    }

    getStateVars() {
        return {
            sourceConfig: this.sourceConfig,
            destConfig: this.destConfig,
            repConfig: this.repConfig,
            destHosts: this.destHosts,
            sourceHTTPAgent: this.sourceHTTPAgent,
            destHTTPAgent: this.destHTTPAgent,
            vaultclientCache: this.vaultclientCache,
            accountCredsCache: this.accountCredsCache,
            logger: this.logger,
        };
    }

    start() {
        const consumer = new BackbeatConsumer({
            zookeeper: { connectionString: this.zkConfig.connectionString },
            topic: this.repConfig.topic,
            groupId: this.repConfig.queueProcessor.groupId,
            concurrency: this.repConfig.queueProcessor.concurrency,
            queueProcessor: this.processKafkaEntry.bind(this),
            fetchMaxBytes: CONSUMER_FETCH_MAX_BYTES,
        });
        consumer.on('error', () => {});
        consumer.subscribe();

        this.logger.info('queue processor is ready to consume ' +
                         'replication entries');
    }

    /**
     * Proceed to the replication of an object given a kafka
     * replication queue entry
     *
     * @param {object} kafkaEntry - entry generated by the queue populator
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
        if (sourceEntry instanceof BucketQueueEntry) {
            if (this.echoMode) {
                task = new EchoBucket(this);
            }
            // ignore bucket entry if echo mode disabled
        } else if (sourceEntry instanceof ObjectQueueEntry) {
            const multipleBackends = ['aws_s3', 'azure'];
            const storageType = sourceEntry.getReplicationStorageType();
            if (multipleBackends.includes(storageType)) {
                task = new MultipleBackendTask(this);
            } else {
                task = new ReplicateObject(this);
            }
        }
        if (task) {
            return this.taskScheduler.push({ task, entry: sourceEntry },
                                           sourceEntry.getCanonicalKey(),
                                           done);
        }
        this.logger.debug('skip source entry',
                          { entry: sourceEntry.getLogInfo() });
        return process.nextTick(done);
    }
}

module.exports = QueueProcessor;
