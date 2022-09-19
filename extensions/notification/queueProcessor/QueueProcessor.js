'use strict'; // eslint-disable-line

const { EventEmitter } = require('events');
const Logger = require('werelogs').Logger;
const async = require('async');
const assert = require('assert');
const util = require('util');

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const NotificationDestination = require('../destination');
const configUtil = require('../utils/config');
const messageUtil = require('../utils/message');
const NotificationConfigManager = require('../NotificationConfigManager');

class QueueProcessor extends EventEmitter {
    /**
     * Create a queue processor object to activate notification from a
     * kafka topic dedicated to dispatch messages to a destination/target.
     *
     * @constructor
     * @param {Object} mongoConfig - mongodb connnection configuration object
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
     * @param {String} destinationConfig.type - destination type
     * @param {String} destinationConfig.host - destination host
     * @param {String} destinationConfig.auth - destination auth configuration
     * @param {String} destinationId - resource name/id of destination
     * @param {Object} destinationAuth - destination authentication config
     */
    constructor(mongoConfig, kafkaConfig, notifConfig, destinationId,
        destinationAuth) {
        super();
        this.mongoConfig = mongoConfig;
        this.kafkaConfig = kafkaConfig;
        this.notifConfig = notifConfig;
        this.destinationId = destinationId;
        this.destinationConfig
            = notifConfig.destinations.find(dest => dest.resource === destinationId);
        assert(this.destinationConfig, `Invalid destination argument "${destinationId}".` +
        ' Destination could not be found in destinations defined');
        // overwriting destination auth config
        // if it was passed by env var
        if (destinationAuth) {
            this.destinationConfig.auth = destinationAuth;
        }
        this.bnConfigManager = null;
        this._consumer = null;
        this._destination = null;
        // Once the notification manager is initialized
        // this will hold the callback version of the getConfig
        // function of the notification config manager
        this._getConfig = null;

        this.logger = new Logger('Backbeat:Notification:QueueProcessor');
    }

    /**
     * Initializes the NotificationConfigManager
     * @param {Function} done callback
     * @returns {undefined}
     */
    _setupNotificationConfigManager(done) {
        try {
            this.bnConfigManager = new NotificationConfigManager({
                mongoConfig: this.mongoConfig,
                logger: this.logger,
            });
            return this.bnConfigManager.setup(done);
        } catch (err) {
            return done(err);
        }
    }

    /**
     * Sets up the destination for this queue processor
     * @param {string} destinationType destination type
     * (only kafka is supported for now)
     * @param {Function} done callback
     * @returns {undefined}
     */
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
     * @param {function} done callback
     * @return {undefined}
     */
    start(options, done) {
        async.series([
            next => this._setupNotificationConfigManager(next),
            next => this._setupDestination(this.destinationConfig.type, next),
            next => this._destination.init(() => {
                if (options && options.disableConsumer) {
                    this.emit('ready');
                    return next();
                }
                const { groupId, concurrency }
                    = this.notifConfig.queueProcessor;
                const consumerGroupId = `${groupId}-${this.destinationId}`;
                const internalTopic = this.destinationConfig.internalTopic ||
                    this.notifConfig.topic;
                this._consumer = new BackbeatConsumer({
                    kafka: {
                        hosts: this.kafkaConfig.hosts,
                        site: this.kafkaConfig.site,
                    },
                    topic: internalTopic,
                    groupId: consumerGroupId,
                    concurrency,
                    queueProcessor: this.processKafkaEntry.bind(this),
                });
                this._consumer.on('error', err => {
                    this.logger.error('error starting notification consumer',
                    { method: 'QueueProcessor.start', error: err.message });
                    // crash if got error at startup
                    if (!this.isReady()) {
                        return next(err);
                    }
                    return undefined;
                });
                this._consumer.on('ready', () => {
                    this._consumer.subscribe();
                    this.logger.info('queue processor is ready to consume ' +
                        'notification entries');
                    this.emit('ready');
                    return next();
                });
                // callbackify getConfig from notification config manager
                this._getConfig = util.callbackify(this.bnConfigManager
                    .getConfig.bind(this.bnConfigManager));
                return undefined;
            }),
        ], err => {
            if (err) {
                this.logger.error('error starting notification queue processor',
                    { method: 'QueueProcessor.start', error: err.message });
                return done(err);
            }
            return done();
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
        const { bucket, key, eventType } = sourceEntry;
        try {
            return this._getConfig(bucket, (err, notifConfig) => {
                if (err) {
                    this.logger.err('Error while getting notification configuration', {
                        bucket,
                        key,
                        eventType,
                        err,
                    });
                    return done(err);
                }
                if (notifConfig && Object.keys(notifConfig).length > 0) {
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
                        queueConfig: [destBnConf],
                    };
                    this.logger.debug('validating entry', {
                        method: 'QueueProcessor.processKafkaEntry',
                        bucket,
                        key,
                        versionId: sourceEntry.versionId,
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
                            versionId: sourceEntry.versionId,
                            encodedVersionId: eventRecord.versionId,
                            eventType: eventRecord.eventName,
                            eventTime: eventRecord.eventTime,
                            destination: this.destinationId,
                        });
                        return this._destination.send([msg], done);
                    }
                }
                // skip if there is no bucket notification configuration
                return done();
            });
        } catch (error) {
            if (error) {
                this.logger.err('error processing entry', {
                    bucket,
                    key,
                    error,
                });
            }
            return done();
        }
    }

    /**
     * Checks if queue processor is ready to consume
     *
     * @returns {boolean} is queue processor ready
     */
    isReady() {
        return this._consumer && this._consumer.isReady();
    }
}

module.exports = QueueProcessor;
