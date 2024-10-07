'use strict'; // eslint-disable-line

const { EventEmitter } = require('events');
const Logger = require('werelogs').Logger;
const async = require('async');
const assert = require('assert');
const { ZenkoMetrics } = require('arsenal').metrics;
const errors = require('arsenal').errors;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const NotificationDestination = require('../destination');
const configUtil = require('../utils/config');
const messageUtil = require('../utils/message');
const NotificationConfigManager = require('../NotificationConfigManager');
const ZookeeperManager = require('../../../lib/clients/ZookeeperManager');

const processedEvents = ZenkoMetrics.createCounter({
    name: 's3_notification_queue_processor_events_total',
    help: 'Total number of successfully processed events',
    labelNames: ['target', 'eventType'],
});

function onQueueProcessorEventProcessed(destination, eventType) {
    processedEvents.inc({
        target: destination,
        eventType,
    });
}

class QueueProcessor extends EventEmitter {
    /**
     * Create a queue processor object to activate notification from a
     * kafka topic dedicated to dispatch messages to a destination/target.
     *
     * @constructor
     * @param {Object} mongoConfig - mongodb connnection configuration object
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
     * @param {Object} destinationAuth - destination authentication config
     */
    constructor(mongoConfig, zkConfig, kafkaConfig, notifConfig, destinationId,
        destinationAuth) {
        super();
        this.mongoConfig = mongoConfig;
        this.zkConfig = zkConfig;
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
        this.zkClient = null;

        this.logger = new Logger('Backbeat:Notification:QueueProcessor');
    }

    _setupZookeeper(done) {
        if (this.mongoConfig) {
            done();
            return;
        }

        const populatorZkPath = this.notifConfig.zookeeperPath;
        const zookeeperUrl =
            `${this.zkConfig.connectionString}${populatorZkPath}`;
        this.logger.info('opening zookeeper connection for reading ' +
            'bucket notification configuration', { zookeeperUrl });
        this.zkClient = new ZookeeperManager(zookeeperUrl, {
            autoCreateNamespace: this.zkConfig.autoCreateNamespace,
        }, this.logger);

        this.zkClient.once('error', done);
        this.zkClient.once('ready', () => {
            // just in case there would be more 'error' events emitted
            this.zkClient.removeAllListeners('error');
            done();
        });
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
                zkClient: this.zkClient,
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
            next => this._setupZookeeper(next),
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
            return this.bnConfigManager.getConfig(bucket, (err, notifConfig) => {
                if (err) {
                    this.logger.error('Error while getting notification configuration', {
                        bucket,
                        key,
                        eventType,
                        err,
                    });
                    return done(err);
                }
                if (notifConfig && Object.keys(notifConfig).length > 0) {
                    const destBnConf = notifConfig.queueConfig.filter(
                        c => c.queueArn.split(':').pop()
                            === this.destinationId);
                    if (!destBnConf.length) {
                        // skip, if there is no config for the current
                        // destination resource
                        return done();
                    }
                    // pass only destination resource specific config to
                    // validate entry
                    const bnConfig = {
                        queueConfig: destBnConf,
                    };
                    this.logger.debug('validating entry', {
                        method: 'QueueProcessor.processKafkaEntry',
                        bucket,
                        key,
                        versionId: sourceEntry.versionId,
                        eventType,
                        destination: this.destinationId,
                    });
                    const { isValid, matchingConfig } = configUtil.validateEntry(bnConfig, sourceEntry);
                    if (isValid) {
                        // add notification configuration id to the message
                        sourceEntry.configurationId = matchingConfig.id;
                        const message
                            = messageUtil.transformToSpec(sourceEntry);
                        const msg = {
                            // for Kafka keyed partitioning, to map a
                            // particular bucket and key to a partition
                            key: `${bucket}/${key}`,
                            message: JSON.stringify(message),
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
                            matchingConfig,
                        });
                        onQueueProcessorEventProcessed(
                            this.destinationId,
                            eventType,
                        );
                        return this._destination.send([msg], done);
                    }
                }
                // skip if there is no bucket notification configuration
                return done();
            });
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

    /**
     * Checks if queue processor is ready to consume
     *
     * @returns {boolean} is queue processor ready
     */
    isReady() {
        return this._consumer && this._consumer.isReady();
    }

    /**
     * Handle ProbeServer metrics
     *
     * @param {http.HTTPServerResponse} res - HTTP Response to respond with
     * @param {Logger} log - Logger
     * @returns {undefined}
     */
    async handleMetrics(res, log) {
        log.debug('metrics requested');
        res.writeHead(200, {
            'Content-Type': ZenkoMetrics.asPrometheusContentType(),
        });
        const metrics = await ZenkoMetrics.asPrometheus();
        res.end(metrics);
    }
}

module.exports = QueueProcessor;
