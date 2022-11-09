const NotificationDestination = require('./NotificationDestination');
const KafkaProducer = require('./KafkaProducer');
const { ZenkoMetrics } = require('arsenal').metrics;

const notificationSize = ZenkoMetrics.createCounter({
    name: 'notification_queue_processor_notification_size',
    help: 'Total size of the notifications that were sent',
    labelNames: ['target'],
});

const deliveryLag = ZenkoMetrics.createHistogram({
    name: 'notification_queue_processor_notification_delivery_delay_sec',
    help: 'Difference between the time a notification is sent and when it gets delivered',
    labelNames: ['target', 'status'],
    buckets: [0.001, 0.01, 1, 10, 100, 1000, 10000],
});

function onNotificationSent(target, status, messages, delay) {
    if (status === 'success') {
        // Update total size of notifications sent
        const messageSize = new TextEncoder().encode(JSON.stringify(messages)).length;
        notificationSize.inc({
            target,
        }, messageSize);
    }
    if (delay) {
        // update notification delivery delay
        deliveryLag.observe({
            target,
            status,
        }, delay);
    }
}

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
        const starTime = Date.now();
        this._notificationProducer.send(messages, error => {
            if (error) {
                this._log.error('error publishing message', {
                    method: 'KafkaNotificationDestination.send',
                    error,
                });
                onNotificationSent(
                    this._destinationConfig.resource,
                    'failure',
                    messages,
                    null,
                );
                return done();
            }
            const delay = (Date.now() - starTime) / 1000;
            onNotificationSent(
                this._destinationConfig.resource,
                'success',
                messages,
                delay,
            );
            return done();
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
