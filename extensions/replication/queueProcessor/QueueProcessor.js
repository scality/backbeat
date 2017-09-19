'use strict'; // eslint-disable-line

const http = require('http');
const fs = require('fs');

const Logger = require('werelogs').Logger;

const errors = require('arsenal').errors;
const RoundRobin = require('arsenal').network.RoundRobin;
const VaultClient = require('vaultclient').Client;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const QueueEntry = require('../utils/QueueEntry');
const ReplicationTaskScheduler = require('./ReplicationTaskScheduler');
const QueueProcessorTask = require('./QueueProcessorTask');
const SetupBucketTask = require('./SetupBucketTask');

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

        this.logger = new Logger('Backbeat:Replication:QueueProcessor');

        // global variables
        // TODO: for SSL support, create HTTPS agents instead
        this.sourceHTTPAgent = new http.Agent({ keepAlive: true });
        this.destHTTPAgent = new http.Agent({ keepAlive: true });

        // FIXME support multiple destination sites
        if (destConfig.bootstrapList.length > 0) {
            this.destHosts =
                new RoundRobin(destConfig.bootstrapList[0].servers,
                               { defaultPort: 80 });
            if (destConfig.bootstrapList[0].echo) {
                this.logger.info('starting in echo mode');
                this._loadAdminCreds();
                this.accountCredsCache = {};
            }
        } else {
            this.destHosts = null;
        }

        if (sourceConfig.auth.type === 'role') {
            const { host, port, adminPort } = sourceConfig.auth.vault;
            this.sourceS3Vault = new VaultClient(host, port);
            if (this.adminCreds) {
                this.sourceAdminVault = new VaultClient(
                    host, adminPort,
                    undefined, undefined, undefined, undefined,
                    undefined,
                    this.adminCreds.accessKey,
                    this.adminCreds.secretKey);
            }
        }
        if (destConfig.auth.type === 'role') {
            // vault client cache per destination
            this.destVaults = {};
        }

        this.taskScheduler = new ReplicationTaskScheduler(
            (ctx, done) => ctx.task.processQueueEntry(ctx.entry, done));
    }

    _loadAdminCreds() {
        const adminCredsJSON = fs.readFileSync('conf/admin-backbeat.json');
        const adminCredsObj = JSON.parse(adminCredsJSON);
        const accessKey = Object.keys(adminCredsObj)[0];
        const secretKey = adminCredsObj[accessKey];
        this.adminCreds = { accessKey, secretKey };
    }

    getStateVars() {
        return {
            sourceConfig: this.sourceConfig,
            destConfig: this.destConfig,
            repConfig: this.repConfig,
            destHosts: this.destHosts,
            sourceHTTPAgent: this.sourceHTTPAgent,
            destHTTPAgent: this.destHTTPAgent,
            sourceAdminVault: this.sourceAdminVault,
            sourceS3Vault: this.sourceS3Vault,
            destVaults: this.destVaults,
            adminCreds: this.adminCreds,
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
        // FIXME support multiple destinations
        if ((this.destConfig.bootstrapList.length === 0 ||
             !this.destConfig.bootstrapList[0].echo) &&
            sourceEntry.isPutBucketOp()) {
            // ignore
            return done();
        }
        const task = sourceEntry.isPutBucketOp() ?
                  new SetupBucketTask(this) :
                  new QueueProcessorTask(this);
        return this.taskScheduler.push(
            { task, entry: sourceEntry },
            `${sourceEntry.getBucket()}/${sourceEntry.getObjectVersionedKey()}`,
            done);
    }
}

module.exports = QueueProcessor;
