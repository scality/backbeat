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
        const { host, topic, auth } = this._destinationConfig;
        const producer = new KafkaProducer({
            kafka: { hosts: host },
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
     * @param {function} done - callback when delivery report has been received
     * @return {undefined}
     */
    send(messages, done) {
        this._notificationProducer.send(messages, error => {
            if (error) {
                const { host, topic } = this._destinationConfig;
                this._log.error('error in message delivery to external Kafka destination', {
                    method: 'KafkaNotificationDestination.send',
                    host,
                    topic,
                    error: error.message,
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
