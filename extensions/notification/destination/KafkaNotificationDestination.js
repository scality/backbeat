const NotificationDestination = require('./NotificationDestination');
const KafkaProducer = require('./KafkaProducer');

class KafkaNotificationDestination extends NotificationDestination {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.destConfig - destination-specific
     *   configuration object
     * @param {Logger} params.logger - logger object
     */
    constructor(params) {
        super(params);
        this._notificationProducer = null;
    }

    _setupProducer(done) {
        const { host, port, topic, auth } = this._destinationConfig;
        const producer = new KafkaProducer({
            kafka: { hosts: `${host}:${port}` },
            topic,
            auth,
        });
        producer.once('error', done);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this._log.error('error from kafka producer', {
                    topic,
                    error: err,
                });
            });
            this._notificationProducer = producer;
            done();
        });
    }

    /**
     * Init/setup/authenticate the notification client
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    init(done) {
        this._setupProducer(err => {
            if (err) {
                this._log.info('error setting up kafka notif destination',
                    { error: err.message });
                done(err);
            }
            done();
        });
    }

    /**
     * Process entry in the sub-class and send it
     *
     * @param {Object[]} messages - message to be sent
     * @param {function} done - callback
     * @return {undefined}
     */
    send(messages, done) {
        this._notificationProducer.send(messages, error => {
            if (error) {
                this._log.error('error publishing message', {
                    method: 'KafkaNotificationDestination.send',
                    error,
                });
            }
            done();
        });
    }

    /**
     * Stop the notification client
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    stop(done) {
        if (!this._notificationProducer) {
            return setImmediate(done);
        }
        return this._notificationProducer.close(done);
    }
}

module.exports = KafkaNotificationDestination;
