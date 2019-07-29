const { EventEmitter } = require('events');
const kafka = require('node-rdkafka');
const assert = require('assert');
const async = require('async');
const joi = require('@hapi/joi');
const Logger = require('werelogs').Logger;

const BackbeatProducer = require('./BackbeatProducer');
const OffsetLedger = require('./OffsetLedger');
const KafkaBacklogMetrics = require('./KafkaBacklogMetrics');

// controls the number of messages to process in parallel
const CONCURRENCY_DEFAULT = 1;
const CLIENT_ID = 'BackbeatConsumer';

const { withTopicPrefix } = require('./util/topic');

class BackbeatConsumer extends EventEmitter {

    /**
     * constructor
     * @param {Object} config - config
     * @param {string} config.topic - Kafka topic to subscribe to
     * @param {function} config.queueProcessor - function to invoke to
     * process an item in a queue (see doc of
     * this.onEntryCommittable() if it's desired not to allow
     * committing the consumer offset immediately after this function
     * calls its callback).
     * @param {string} config.groupId - consumer group id. Messages are
     * distributed among multiple consumers belonging to the same group
     * @param {Object} [config.zookeeper] - zookeeper endpoint config
     * (only needed if config.kafka.backlogMetrics is set)
     * @param {string} config.zookeeper.connectionString - zookeeper
     * connection string as "host:port[/chroot]" (only needed if
     * config.kafka.backlogMetrics is set)
     * @param {Object} config.kafka - kafka connection config
     * @param {string} config.kafka.hosts - kafka hosts list
     * as "host:port[,host:port...]"
     * @param {object} [config.kafka.backlogMetrics] - param object to
     * publish kafka topic metrics to zookeeper (disabled if param object
     * is not set)
     * @param {string} config.kafka.backlogMetrics.zkPath - zookeeper base
     * path to publish metrics to
     * @param {boolean} [config.kafka.backlogMetrics.intervalS=60] -
     * interval in seconds between iterations of kafka metrics
     * publishing task
     * @param {string} [config.fromOffset] - valid values latest/earliest/none
     * @param {number} [config.concurrency] - represents the number of entries
     * that can be processed in parallel
     * @param {number} [config.fetchMaxBytes] - max. bytes to fetch in a
     * fetch loop
     * @param {boolean} [config.canary=false] - true to send a canary
     * message to bootstrap partition offsets (useful for pause/resume
     * functionality to work)
     * @param {boolean} [config.bootstrap=false] - TEST ONLY: true to
     * bootstrap the consumer with test messages until it starts
     * consuming them
     */
    constructor(config) {
        super();

        const configJoi = {
            zookeeper: joi.object({
                connectionString: joi.string().required(),
            }).when('kafka.backlogMetrics', { is: joi.exist(),
                                            then: joi.required() }),
            kafka: joi.object({
                hosts: joi.string().required(),
                backlogMetrics: {
                    zkPath: joi.string().required(),
                    intervalS: joi.number().default(60),
                },
            }).required(),
            topic: joi.string().required(),
            groupId: joi.string().required(),
            queueProcessor: joi.func(),
            fromOffset: joi.alternatives().try('latest', 'earliest', 'none'),
            concurrency: joi.number().greater(0).default(CONCURRENCY_DEFAULT),
            fetchMaxBytes: joi.number(),
            canary: joi.boolean().default(false),
            bootstrap: joi.boolean().default(false),
        };
        const validConfig = joi.attempt(config, configJoi,
                                        'invalid config params');

        const { zookeeper, kafka, topic, groupId, queueProcessor,
                fromOffset, concurrency, fetchMaxBytes,
                canary, bootstrap } = validConfig;

        this._zookeeperEndpoint = zookeeper && zookeeper.connectionString;
        this._kafkaHosts = kafka.hosts;
        this._kafkaBacklogMetricsConfig = kafka.backlogMetrics;
        this._fromOffset = fromOffset;
        this._log = new Logger(CLIENT_ID);
        this._topic = withTopicPrefix(topic);
        this._groupId = groupId;
        this._queueProcessor = queueProcessor;
        this._concurrency = concurrency;
        this._fetchMaxBytes = fetchMaxBytes;
        this._canary = canary;
        this._bootstrap = bootstrap;
        this._offsetLedger = new OffsetLedger();

        this._processingQueue = null;
        this._messagesConsumed = 0;
        // this variable represents how many kafka messages have been
        // requested without having been received yet, i.e. still
        // being fetched by this._consumer.consume()
        this._nConsumePendingRequests = 0;
        this._consumer = null;
        this._consumerReady = false;
        this._bootstrapping = false;
        this._kafkaBacklogMetrics = null;
        this._kafkaBacklogMetricsReady = false;
        this._publishOffsetsCronTimer = null;
        this._publishOffsetsCronActive = false;
        this._consumedEventTimeout = null;

        this._init();
        return this;
    }

    _init() {
        if (this._bootstrap) {
            this._consumerReady = true;
        } else {
            this._initConsumer();
        }
        if (this._kafkaBacklogMetricsConfig) {
            this._initKafkaBacklogMetrics();
        } else {
            this._kafkaBacklogMetricsReady = true;
        }
        process.nextTick(this._checkIfReady.bind(this));
    }

    _initConsumer() {
        const consumerParams = {
            'metadata.broker.list': this._kafkaHosts,
            'group.id': this._groupId,
            // we manage stored offsets based on the highest
            // contiguous offset fully processed by a worker, so
            // disabling automatic offset store is needed
            'enable.auto.offset.store': false,
            // this function is called periodically based on
            // auto-commit of stored offsets
            'offset_commit_cb': this._onOffsetCommit.bind(this),
        };
        const topicParams = {};
        if (this._fromOffset !== undefined) {
            topicParams['auto.offset.reset'] = this._fromOffset;
        }
        if (this._fetchMaxBytes !== undefined) {
            consumerParams['fetch.message.max.bytes'] = this._fetchMaxBytes;
        }
        this._consumer = new kafka.KafkaConsumer(consumerParams, topicParams);
        this._consumer.connect({ timeout: 10000 }, () => {
            const opts = {
                topic: withTopicPrefix('backbeat-sanitycheck'),
                timeout: 10000,
            };
            this._consumer.getMetadata(opts, err => {
                if (err) {
                    this.emit('error', err);
                }
            });
        });
        return this._consumer.once('ready', () => {
            this._consumerReady = true;
            this._checkIfReady();
        });
    }

    _initKafkaBacklogMetrics() {
        this._kafkaBacklogMetrics = new KafkaBacklogMetrics(
            this._zookeeperEndpoint, this._kafkaBacklogMetricsConfig);
        this._kafkaBacklogMetrics.init();
        this._kafkaBacklogMetrics.once('ready', () => {
            this._kafkaBacklogMetricsReady = true;
            this._checkIfReady();
        });
    }

    _checkIfReady() {
        if (this._consumerReady && this._kafkaBacklogMetricsReady) {
            if (this._bootstrap) {
                if (!this._bootstrapping) {
                    this._bootstrapConsumer();
                }
            } else {
                this._onReady();
            }
        }
    }

    _onReady() {
        this.emit('ready');
        if (this._kafkaBacklogMetricsConfig) {
            this._publishOffsetsCronTimer =
                setInterval(this._publishOffsetsCron.bind(this),
                            this._kafkaBacklogMetricsConfig.intervalS * 1000);
        }
    }

    isReady() {
        let kMetrics = null;
        let cs = null;
        if (this._kafkaBacklogMetricsConfig) {
            kMetrics = this._kafkaBacklogMetrics.isReady();
        } else {
            kMetrics = true;
        }

        if (this._bootstrap) {
            cs = true;
        } else {
            cs = this._consumer && this._consumer.isConnected();
        }
        return kMetrics && cs;
    }

    /**
    * subscribe to messages from a topic
    * Once subscribed, the consumer does a fetch from the topic with new
    * messages. Each fetch loop can contain one or more messages, so the fetch
    * is paused until the current queue of tasks are processed. Once the task
    * queue is empty, the current offset is committed and the fetch is resumed
    * to get the next batch of messages
    * @param {Boolean} [paused] - optional field. If true, kafka consumer should
    *   not subscribe to its topic
    * @return {this} current instance
    */
    subscribe(paused) {
        if (!paused) {
            this._consumer.subscribe([this._topic]);
        } else {
            this._log.debug(`consumer is paused for topic ${this._topic}`);
        }

        if (this._canary) {
            this._sendCanary(() => {
                this.emit('canary');
            });
        }

        this._processingQueue = async.queue(
            this._queueProcessor, this._concurrency);

        this._consumer.on('event.error', error => {
            // This is a bit hacky: the "broker transport failure"
            // error occurs when the kafka broker reaps the idle
            // connections every few minutes, and librdkafka handles
            // reconnection automatically anyway, so we ignore those
            // harmless errors (moreover with the current
            // implementation there's no way to access the original
            // error code, so we match the message instead).
            if (!['broker transport failure',
                  'all broker connections are down']
                .includes(error.message)) {
                this._log.error('consumer error', {
                    error,
                    topic: this._topic,
                });
                this.emit('error', error);
            }
        });

        // trigger initial consumption
        this._tryConsume();
        return this;
    }

    /**
     * Get how many messages we may attempt to consume at this time,
     * considering the maximum concurrency, current processing queue
     * length and message retrievals still pending.
     *
     * @return {integer} a strictly positive number representing the
     * number of new messages that the pipeline is ready to process,
     * or zero if the processing pipeline is 100% busy
     */
    _getAvailableSlotsInPipeline() {
        const nSlots = this._concurrency
              - this._processingQueue.running()
              - this._processingQueue.length()
              - this._nConsumePendingRequests;
        return nSlots > 0 ? nSlots : 0;
    }

    _tryConsume() {
        // use non-flowing mode of consumption to add some flow
        // control: explicit consumption of messages is required,
        // needs explicit polling to get new messages

        const nNewConsumeRequests = this._getAvailableSlotsInPipeline();
        if (nNewConsumeRequests === 0) {
            // processing pipeline is 100% busy, do not attempt to consume
            return undefined;
        }
        this._nConsumePendingRequests += nNewConsumeRequests;
        return this._consumer.consume(nNewConsumeRequests, (err, entries) => {
            this._nConsumePendingRequests -= nNewConsumeRequests;
            if (!err) {
                entries.forEach(entry => {
                    const { topic, partition, offset, key, timestamp } = entry;
                    this._log.debug('marked consumed entry', {
                        entry: { topic, partition, offset,
                                 key: key && key.toString(), timestamp },
                    });
                    if (topic === undefined ||
                        partition === undefined ||
                        offset === undefined) {
                        this._log.error('"topic" or "partition" or "offset" ' +
                                        'is undefined in entry', {
                                            entry: {
                                                topic, partition, offset,
                                                key: key && key.toString(),
                                                timestamp,
                                            },
                                        });
                        return undefined;
                    }
                    this._offsetLedger.onOffsetConsumed(
                        entry.topic, entry.partition, entry.offset);
                    this._messagesConsumed++;
                    this._processingQueue.push(entry, (err, completionArgs) => {
                        this._onEntryProcessingDone(err, entry, completionArgs);
                    });
                    // update Zenko metrics with the latest consumed
                    // message timestamp, to later allow computing
                    // backlog metrics on demand
                    KafkaBacklogMetrics.onMessageConsumed(
                        entry.topic, entry.partition, this._groupId,
                        entry.timestamp / 1000);
                    return undefined;
                });
            }
            if (err || entries.length === 0) {
                this._log.debug('no message is available yet, retry in 1s');
                setTimeout(this._tryConsume.bind(this), 1000);
            }
        });
    }

    _onEntryProcessingDone(err, entry, completionArgs) {
        const { topic, partition, offset, key, timestamp } = entry;
        this._log.debug('finished processing of consumed entry', {
            method: 'BackbeatConsumer.subscribe',
            topic, partition, offset, key, timestamp,
        });
        if (err) {
            this._log.error('error processing an entry', {
                error: err,
                entry: { topic, partition, offset, key, timestamp },
            });
            this.emit('error', err, entry);
        }
        // use setTimeout to do gathering and emit less events
        if (!this._consumedEventTimeout) {
            this._consumedEventTimeout = setTimeout(() => {
                if (this._messagesConsumed > 0) {
                    this.emit('consumed', this._messagesConsumed);
                    this._messagesConsumed = 0;
                }
                this._consumedEventTimeout = null;
            }, 100);
        }
        if (!(completionArgs && completionArgs.committable === false)) {
            this.onEntryCommittable(entry);
        }
        // check whether we may get new messages now that the queue
        // can accomodate more work
        process.nextTick(() => this._tryConsume());
    }

    _onOffsetCommit(err, topicPartitions) {
        if (err) {
            // NO_OFFSET is a "soft error" meaning that the same
            // offset is already committed, which occurs because of
            // auto-commit (e.g. if nothing was done by the producer
            // on this partition since last commit).
            if (err === kafka.CODES.ERRORS.ERR__NO_OFFSET) {
                return undefined;
            }
            this._log.error('error committing offsets to kafka',
                            { errorCode: err, topicPartitions });
        }
        this._log.debug('commit offsets callback',
                        { topicPartitions });
        return undefined;
    }

    /**
     * Function to be called when safe to commit the consumer offset
     * of the given entry
     *
     * When a task completes from queueProcessor function (constructor
     * param), one has to call the callback with an extra argument
     * "committable" set to false, like "done(null, { committable:
     * false })". This will prevent the consumer from committing any
     * consumer group offset on or after this entry for the entry's
     * partition, until this.onEntryCommittable(entry) is called when
     * safe or desired. Older entries in the same partition may still
     * hold commits on this partition if they have not been advertised
     * as being committable yet.
     *
     * Note: this.onEntryCommittable() *must* be called in order to
     * allow progress committing the consumer offsets, whenever the
     * "committable: false" option has been passed to the task
     * callback. It may be called otherwise but will have no effect
     * because the entry will already have been considered committable
     * at task completion time.
     *
     * @param {object} entry - entry passed originally as first
     * parameter to the "queueProcessor" function
     * @return {undefined}
     */
    onEntryCommittable(entry) {
        const { topic, partition, offset, key, timestamp } = entry;
        // record the highest committable offset and let auto-commit
        // persist it periodically
        const committableOffset =
              this._offsetLedger.onOffsetProcessed(topic, partition, offset);
        this._log.debug('marked committable entry', {
            entry: { topic, partition, offset,
                     key: key && key.toString(), timestamp },
            committableOffset,
        });
        // ensure consumer is active when calling offsetsStore() on
        // it, to avoid raising an exception (invalid state)
        if (committableOffset !== null && !this.isPaused()) {
            this._consumer.offsetsStore([{ topic, partition,
                                           offset: committableOffset }]);
        }
    }

    _publishOffsetsCron(cb) {
        if (this._publishOffsetsCronActive) {
            // skipping
            if (cb) {
                return process.nextTick(cb);
            }
            return undefined;
        }
        this._publishOffsetsCronActive = true;
        return this._kafkaBacklogMetrics.publishConsumerBacklog(
            this._consumer, this._topic, this._groupId, err => {
                this._publishOffsetsCronActive = false;
                if (cb) {
                    // used for shutdown
                    cb(err);
                }
            });
    }

    /**
     * get metadata from kafka topics
     * @param {object} params - call params
     * @param {string} params.topic - topic name
     * @param {number} params.timeout - timeout for the request
     * @param {function} cb - callback: cb(err, response)
     * @return {undefined}
     */
    getMetadata(params, cb) {
        this._consumer.getMetadata(params, cb);
    }

    /**
     * Bootstrap consumer by periodically sending bootstrap messages
     * and wait until it's receiving newly produced messages in a
     * timely fashion.
     * @return {undefined}
     */
    _bootstrapConsumer() {
        const self = this;
        let lastBootstrapId;
        let producer; // eslint-disable-line prefer-const
        let producerTimer; // eslint-disable-line prefer-const
        let consumerTimer; // eslint-disable-line prefer-const
        function consumeCb(err, messages) {
            if (err) {
                return undefined;
            }
            messages.forEach(message => {
                const bootstrapId = JSON.parse(message.value).bootstrapId;
                if (bootstrapId) {
                    self._log.info('bootstraping backbeat consumer: ' +
                                   'received bootstrap message',
                                   { bootstrapId });
                    if (bootstrapId === lastBootstrapId) {
                        self._log.info('backbeat consumer is bootstrapped');
                        clearInterval(producerTimer);
                        clearInterval(consumerTimer);
                        self._consumer.commit();
                        self._consumer.unsubscribe();
                        producer.close(() => {
                            self._bootstrapping = false;
                            self._onReady();
                        });
                    }
                }
            });
            return undefined;
        }
        assert.strictEqual(this._consumer, null);
        producer = new BackbeatProducer({
            kafka: { hosts: this._kafkaHosts },
            topic: this._topic,
        });
        producer.on('ready', () => {
            producerTimer = setInterval(() => {
                lastBootstrapId = `${Math.round(Math.random() * 1000000)}`;
                const contents = `{"bootstrapId":"${lastBootstrapId}"}`;
                this._log.info('bootstraping backbeat consumer: ' +
                               'sending bootstrap message',
                               { contents });
                producer.send([{ key: 'bootstrap',
                                 message: contents }],
                              () => {});
                if (!this._consumer) {
                    setTimeout(() => {
                        this._bootstrapping = true;
                        this._initConsumer();
                        this._consumer.on('ready', () => {
                            this._consumer.subscribe([this._topic]);
                            consumerTimer = setInterval(() => {
                                this._consumer.consume(1, consumeCb);
                            }, 200);
                        });
                    }, 500);
                }
            }, 5000);
        });
    }

    /**
     * pause the kafka consumer
     * @param {string} site - name of site
     * @return {undefined}
     */
    pause(site) {
        // Use of KafkaConsumer#pause did not work. Using alternative
        // of unsubscribe/subscribe
        this._consumer.unsubscribe();
        this._log.debug(`paused consumer for location: ${site}`, {
            method: 'BackbeatConsumer.pause',
        });
    }

    /**
     * resume the kafka consumer
     * @param {string} site - name of site
     * @return {undefined}
     */
    resume(site) {
        // if not subscribed, then subscribe
        if (this._consumer.subscription().length === 0) {
            this._consumer.subscribe([this._topic]);
            this._log.debug(`resumed consumer for location: ${site}`, {
                method: 'BackbeatConsumer.resume',
            });
        }
    }

    /**
     * tells whether the consumer is in paused state
     *
     * Note that right now this duplicates getServiceStatus(), but
     * this one is meant to be backported to the oldest release that
     * needs it, and is potentially more specific than
     * getServiceStatus().
     *
     * @return {boolean} true if paused
     */
    isPaused() {
        return this._consumer.subscription().length === 0;
    }

    /**
     * check if the kafka consumer is active or paused
     * @return {boolean} if false, consumer is paused
     */
    getServiceStatus() {
        const subscriptions = this._consumer.subscription();
        return subscriptions.length > 0;
    }

    /**
     * Helper method to wait for consumers to be assigned to partitions before
     * proceeding to send canary messages.
     * @param {number} wait - current accumulated wait time
     * @param {callback} cb - callback to invoke
     * @return {undefined}
     */
    _waitForAssignment(wait, cb) {
        setTimeout(() => {
            const assignments = this._consumer.assignments();
            const topicDetails = this._consumer._metadata.topics
                  .find(topic => topic.name === this._topic);
            if (assignments.length === 0 || !topicDetails) {
                if (wait > 50000) {
                    return cb(true);
                }
                return this._waitForAssignment(wait + 2000, cb);
            }
            return cb();
        }, 2000);
    }

    /**
     * On start, send canary (noop) messages to set partition offsets for this
     * consumer group. Necessary to avoid pause/resume bug where new locations
     * on pause will not be able to queue entries as no current offset to
     * determine number of entries (lag) to consume on resume.
     * @param {callback} cb - callback to invoke
     * @return {undefined}
     */
    _sendCanary(cb) {
        return this._waitForAssignment(0, err => {
            if (err) {
                this._log.debug('could not send canary on consumer init, ' +
                'waiting for consumer assignment to partition took too long', {
                    method: 'BackbeatConsumer._sendCanary',
                });
                return cb();
            }
            const topicDetails = this._consumer._metadata.topics
                  .find(topic => topic.name === this._topic);
            const entries = topicDetails.partitions.map(p => ({
                key: 'canary',
                message: '{"canary":true}',
                partition: p.id,
            }));
            const producer = new BackbeatProducer({
                kafka: { hosts: this._kafkaHosts },
                topic: this._topic,
            });
            return producer.on('ready', () => {
                producer.send(entries, () => {
                    producer.close(cb);
                });
            });
        });
    }

    /**
     * Get the offset ledger attached to this consumer (for testing purpose)
     *
     * @return {OffsetLedger} - offset ledger object
     */
    getOffsetLedger() {
        return this._offsetLedger;
    }

    /**
     * force commit the current offset and close the client connection
     * @param {callback} cb - callback to invoke
     * @return {undefined}
     */
    close(cb) {
        if (this._publishOffsetsCronTimer) {
            clearInterval(this._publishOffsetsCronTimer);
            this._publishOffsetsCronTimer = null;
        }
        if (this._publishOffsetsCronActive) {
            return setTimeout(() => this.close(cb), 1000);
        }
        return async.series([
            next => {
                if (this._consumer) {
                    this._consumer.commit();
                }
                if (this._kafkaBacklogMetricsConfig) {
                    // publish offsets to zookeeper
                    return this._publishOffsetsCron(() => next());
                }
                return process.nextTick(next);
            },
            next => {
                if (this._zookeeper) {
                    this._zookeeper.close();
                }
                if (this._consumer) {
                    this._consumer.unsubscribe();
                    this._consumer.disconnect();
                    this._consumer.on('disconnected', () => next());
                } else {
                    process.nextTick(next);
                }
            },
        ], () => cb());
    }
}

module.exports = BackbeatConsumer;
