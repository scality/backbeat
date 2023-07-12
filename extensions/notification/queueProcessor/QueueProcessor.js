'use strict'; // eslint-disable-line

const { EventEmitter } = require('events');
const Logger = require('werelogs').Logger;
const async = require('async');
const errors = require('arsenal').errors;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const NotificationDestination = require('../destination');
const zookeeper = require('../../../lib/clients/zookeeper');
const configUtil = require('../utils/config');
const messageUtil = require('../utils/message');
const NotificationConfigManager = require('../NotificationConfigManager');

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
     * @param {String} [notifConfig.queueProcessor.concurrency=1000] -
     * how many notifications can be processed concurrently, between
     * the time they are consumed from the internal Kafka queue and
     * the time a delivery report is received from the external Kafka
     * broker (see also {@link destinationConfig.pollIntervalMs})
     * @param {String} notifConfig.queueProcessor.retryTimeoutS -
     *   number of seconds before giving up retries of an entry
     * @param {Object} destinationConfig - destination configuration object
     * @param {String} destinationConfig.type - destination type
     * @param {String} destinationConfig.host - destination host
     * @param {String} destinationConfig.auth - destination auth configuration
     * @param {number} [destinationConfig.pollIntervalMs=2000] - for
     * Kafka destinations: producer poll interval between delivery
     * report fetches, in milliseconds. Reducing the delay could
     * reduce latency in message delivery under high load and/or slow
     * external Kafka cluster, at the expense of more polling overhead
     * IMPORTANT: make sure {@link notifConfig.queueProcessor.concurrency}
     * is set to a value high enough so that "concurrency / poll_interval_seconds"
     * which is the maximum notifications per second per notification
     * processor, is not below the required throughput
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
        this.bnConfigManager = null;
        this._consumer = null;
        this._destination = null;

        this.logger = new Logger('Backbeat:Notification:QueueProcessor');
    }

    _setupZookeeper(done) {
        const populatorZkPath = this.notifConfig.zookeeperPath;
        const zookeeperUrl =
            `${this.zkConfig.connectionString}${populatorZkPath}`;
        this.logger.info('opening zookeeper connection for reading ' +
            'bucket notification configuration', { zookeeperUrl });
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

    _setupNotificationConfigManager(done) {
        try {
            this.bnConfigManager = new NotificationConfigManager({
                zkClient: this.zkClient,
                logger: this.logger,
            });
            return this.bnConfigManager.init(done);
        } catch (err) {
            return done(err);
        }
    }

    _setupDestination(destinationType, done) {
        const Destination = NotificationDestination[destinationType];
        const params = {
            destConfig: this.destinationConfig,
            logger: this.logger,
        };
        this._destination = new Destination(params);
        done();
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
            next => this._setupNotificationConfigManager(next),
            next => this._setupDestination(this.destinationConfig.type, next),
            next => this._destination.init(() => {
                if (options && options.disableConsumer) {
                    this.emit('ready');
                    return undefined;
                }
                const { groupId, concurrency, logConsumerMetricsIntervalS }
                    = this.notifConfig.queueProcessor;
                const consumerGroupId = `${groupId}-${this.destinationId}`;
                this._consumer = new BackbeatConsumer({
                    kafka: {
                        hosts: this.kafkaConfig.hosts,
                        site: this.kafkaConfig.site,
                    },
                    topic: this.notifConfig.topic,
                    groupId: consumerGroupId,
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
        let sourceEntry;
        try {
            sourceEntry = JSON.parse(kafkaEntry.value);
        } catch (error) {
            this.logger.error('error parsing JSON entry', {
                error: error.message,
                errorStack: error.stack,
            });
            return done(errors.InternalError);
        }
        const { bucket, key, eventType } = sourceEntry;
        try {
            const config = this.bnConfigManager.getConfig(bucket);
            if (config && Object.keys(config).length > 0) {
                const notifConfig = config.notificationConfiguration;
                const destBnConf = notifConfig.queueConfig.find(
                    c => c.queueArn.split(':').pop()
                        === this.destinationId);
                if (!destBnConf) {
                    // skip, if there is no config for the current
                    // destination resource
                    return done();
                }
                // pass only destination resource specific config to
                // validate entry
                const bnConfig = {
                    bucket,
                    notificationConfiguration: {
                        queueConfig: [destBnConf],
                    },
                };
                this.logger.debug('validating entry', {
                    method: 'QueueProcessor.processKafkaEntry',
                    bucket,
                    key,
                    eventType,
                    destination: this.destinationId,
                });
                if (configUtil.validateEntry(bnConfig, sourceEntry)) {
                    // add notification configuration id to the message
                    sourceEntry.configurationId = destBnConf.id;
                    const message
                        = messageUtil.transformToSpec(sourceEntry);
                    const msg = {
                        // for Kafka keyed partitioning, to map a
                        // particular bucket and key to a partition
                        key: `${bucket}/${key}`,
                        message,
                    };
                    const msgDesc = 'sending message to external destination';
                    const eventRecord = message.Records[0];
                    this.logger.info(msgDesc, {
                        method: 'QueueProcessor.processKafkaEntry',
                        bucket,
                        key,
                        eventType: eventRecord.eventName,
                        eventTime: eventRecord.eventTime,
                        destination: this.destinationId,
                    });
                    return this._destination.send([msg], done);
                }
                return done();
            }
            // skip if there is no bucket notification configuration
            return done();
        } catch (error) {
            if (error) {
                this.logger.error('error processing entry', {
                    bucket,
                    key,
                    error: error.message,
                    errorStack: error.stack,
                });
            }
            return done();
        }
    }
}

module.exports = QueueProcessor;
