'use strict'; // eslint-disable-line

const { EventEmitter } = require('events');
const Logger = require('werelogs').Logger;
const async = require('async');

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const NotificationDestination = require('../destination');
const zookeeper = require('../../../lib/clients/zookeeper');
const configUtil = require('../utils/config');
const safeJsonParse = require('../utils/safeJsonParse');
const notifConstants = require('../constants');

class QueueProcessor extends EventEmitter {

    /**
     * Create a queue processor object to activate notification from a
     * kafka topic dedicated to dispatch messages to a destination/target.
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} notifConfig - notification configuration object
     * @param {String} notifConfig.topic - notification topic name
     * @param {String} notifConfig.queueProcessor - config object
     *   specific to queue processor
     * @param {String} notifConfig.queueProcessor.groupId - kafka
     *   consumer group ID
     * @param {String} notifConfig.queueProcessor.retryTimeoutS -
     *   number of seconds before giving up retries of an entry
     * @param {Object} destinationConfig - destination configuration object
     * @param {String} destinationConfig.type - destination type
     * @param {String} destinationConfig.host - destination host
     * @param {String} destinationConfig.auth - destination auth configuration
     * @param {String} destinationId - resource name/id of destination
     */
    constructor(zkConfig, kafkaConfig, notifConfig, destinationConfig,
        destinationId) {
        super();
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
        this.notifConfig = notifConfig;
        this.destinationConfig = destinationConfig;
        this.destinationId = destinationId;
        this.zkClient = null;
        this._consumer = null;
        this._destination = null;

        this.logger = new Logger('Backbeat:Notification:QueueProcessor');
    }

    _setupZookeeper(done) {
        const populatorZkPath = this.notifConfig.zookeeperPath;
        const zookeeperUrl =
            `${this.zkConfig.connectionString}${populatorZkPath}`;
        this.logger.info('opening zookeeper connection for persisting ' +
            'populator state',
            { zookeeperUrl });
        this.zkClient = zookeeper.createClient(zookeeperUrl, {
            autoCreateNamespace: this.zkConfig.autoCreateNamespace,
        });
        this.zkClient.connect();
        this.zkClient.once('error', done);
        this.zkClient.once('ready', () => {
            // just in case there would be more 'error' events emitted
            this.zkClient.removeAllListeners('error');
            done();
        });
    }

    _setupDestination(destinationType, done) {
        const Destination = NotificationDestination[destinationType];
        const params = {
            destConfig: this.destinationConfig,
            logger: this.logger,
        };
        this._destination = new Destination(params);
        return this._destination.init(done);
    }

    _getBucketNodeZkPath(bucket) {
        const { zkBucketNotificationPath, zkConfigParentNode }
            = notifConstants;
        return `/${zkBucketNotificationPath}/${zkConfigParentNode}/${bucket}`;
    }

    _getBucketNotifConfig(bucket, done) {
        const method
            = 'notification.QueueProcessor._getBucketNotifConfig';
        const zkPath = this._getBucketNodeZkPath(bucket);
        return this.zkClient.getData(zkPath, (err, data) => {
            if (err && err.name !== 'NO_NODE') {
                this.logger.error('error fetching bucket notification config', {
                    method,
                    error: err,
                });
                return done(err);
            }
            if (data) {
                const { error, result } = safeJsonParse(data);
                if (error) {
                    this.logger.error('invalid config', { method, zkPath, data });
                    return done(null, 1);
                }
                this.logger.debug('fetched bucket notification config', {
                    method,
                    zkPath,
                    data: result,
                });
                return done(null, result);
            }
            return done(null, 0);
        });
    }

    /**
     * Start kafka consumer. Emits a 'ready' even when consumer is ready.
     *
     * Note: for tests, with auto.create.topics.enable option set on
     * kafka container, this will also pre-create the topic.
     *
     * @param {object} [options] options object
     * @param {boolean} [options.disableConsumer] - true to disable
     *   startup of consumer (for testing: one has to call
     *   processQueueEntry() explicitly)
     * @return {undefined}
     */
    start(options) {
        async.series([
            next => this._setupZookeeper(next),
            next => this._setupDestination(this.destinationConfig.type, () => {
                if (options && options.disableConsumer) {
                    this.emit('ready');
                    return undefined;
                }
                const { groupId, concurrency, logConsumerMetricsIntervalS }
                    = this.notifConfig.queueProcessor;
                this._consumer = new BackbeatConsumer({
                    kafka: { hosts: this.kafkaConfig.hosts },
                    topic: this.notifConfig.topic,
                    groupId,
                    concurrency,
                    queueProcessor: this.processKafkaEntry.bind(this),
                    logConsumerMetricsIntervalS,
                });
                this._consumer.on('error', () => { });
                this._consumer.on('ready', () => {
                    this._consumer.subscribe();
                    this.logger.info('queue processor is ready to consume ' +
                        'notification entries');
                    this.emit('ready');
                });
                return next();
            }),
        ], err => {
            if (err) {
                this.logger.info('error starting notification queue processor',
                    { error: err.message });
                return undefined;
            }
            return undefined;
        });
    }

    /**
     * Stop kafka producer and consumer and commit current consumer
     * offset
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    stop(done) {
        if (this._consumer) {
            this._consumer.close(done);
        } else {
            done();
        }
    }

    /**
     * Process kafka entry
     *
     * @param {object} kafkaEntry - entry generated by the queue populator
     * @param {function} done - callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, done) {
        const sourceEntry = JSON.parse(kafkaEntry.value);
        const { bucket, key } = sourceEntry;

        return async.waterfall([
            next => this._getBucketNotifConfig(bucket, next),
            (config, next) => {
                if (config && Object.keys(config).length > 0) {
                    const { error, result } = safeJsonParse(config);
                    if (error) {
                        // skip, invalid configuration
                        return next();
                    }
                    // TODO: handle the following in config util
                    const destBnConf = result.notificationConfiguration.find(
                        c => c.queueConfig.queueArn === this.destinationId);
                    if (!destBnConf) {
                        // skip, if there is no config for the current
                        // destination resource
                        return next();
                    }
                    // pass only destination resource specific config to
                    // validate entry
                    const bnConfig = {
                        bucket,
                        notificationConfiguration: [destBnConf],
                    };
                    if (configUtil.validateEntry(bnConfig, sourceEntry)) {
                        return this._destination.send(sourceEntry, next);
                    }
                    return next();
                }
                // skip if there is no bucket notification configuration
                return next();
            },
        ], error => {
            if (error) {
                this.logger.err('error processing entry', {
                    bucket,
                    key,
                    error,
                });
            }
            return done();
        });
    }
}

module.exports = QueueProcessor;
