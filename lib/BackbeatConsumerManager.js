const { EventEmitter } = require('events');
const async = require('async');

const BackbeatConsumer = require('./BackbeatConsumer');

class BackbeatConsumerManager extends EventEmitter {
    /**
     * Constructor of BackbeatConsumerManagers
     * @param {String} id - manager id
     * @param {Object} consumerParams - object containing BackbeatConsumer params
     * @param {Object} logger - logger
     */
    constructor(id, consumerParams, logger) {
        super();
        this.id = id;
        this._log = logger;
        this._consumerParams = consumerParams;
        this._consumers = {};
        // this._consumersCount = {};
    }

    isReady() {
        return Object.keys(this._consumers).every(id => this._consumers[id].isReady());
    }

    _setupConsumer(consumerId, params, cb) {
        let consumerReady = false;
        const consumer = new BackbeatConsumer(params);
        this._consumers[consumerId] = consumer;

        consumer.on('error', err => {
            if (!consumerReady) {
                this._log.fatal('unable to start consumer', {
                    consumer: consumerId,
                    error: err,
                    method: 'BackbeatConsumerManager._setupConsumer',
                });
                cb(err);
            }
        });

        consumer.on('ready', () => {
            consumerReady = true;
            consumer.subscribe();
            this._log.info('consumer started successfully', {
                consumer: consumerId,
                topic: params.topic,
                method: 'BackbeatConsumerManager._setupConsumer',
            });
            cb();
        });
    }

    getConsumer(id) {
        return this._consumers[id];
    }

    setupConsumers(cb) {
        async.each(
            Object.keys(this._consumerParams),
            (id, done) => this._setupConsumer(id, this._consumerParams[id], done),
            err => {
                if (err) {
                    return cb(err);
                }
                return cb();
            });
    }


    close(cb) {
        async.each(
            Object.keys(this._consumers),
            (id, done) => this._consumers[id].close(done),
            err => {
                if (err) {
                    this._log.error('unable to stop consumers');
                    return cb(err);
                }
                return cb();
            });
    }
}

module.exports = BackbeatConsumerManager;
