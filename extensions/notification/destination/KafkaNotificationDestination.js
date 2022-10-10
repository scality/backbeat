const NotificationDestination = require('./NotificationDestination');
const KafkaProducer = require('./KafkaProducer');
const { ZenkoMetrics } = require('arsenal').metrics;

// Bucket Notification target
const NOTIFICATION_LABEL_DESTINATION =  'target';
// can be one of :
// - 'success' : if notification got delivered successfully
// - 'fail' : if an error happended while sending notification
const NOTIFICATION_LABEL_DELIVERY_STATUS =  'status';

const notificationsSent = ZenkoMetrics.createCounter({
    name: 'notification_queue_processor_notifications_sent',
    help: 'Total number of notifications sent',
    labelNames: [
        NOTIFICATION_LABEL_DESTINATION,
        NOTIFICATION_LABEL_DELIVERY_STATUS
    ],
});

const notificationSize = ZenkoMetrics.createCounter({
    name: 'notification_queue_processor_notification_size',
    help: 'Total size of the notifications that were sent',
    labelNames: [
        NOTIFICATION_LABEL_DESTINATION
    ],
});

const deliveryDelay = ZenkoMetrics.createSummary({
    name: 'notification_queue_processor_notification_delivery_delay_sec',
    help: 'Difference between the time a notification is sent and when it gets delivered',
    labelNames: [
        NOTIFICATION_LABEL_DESTINATION
    ],
});

function onNotificationSent(destination, status, messages, delay) {
    notificationsSent.inc({
        [NOTIFICATION_LABEL_DESTINATION]: destination,
        [NOTIFICATION_LABEL_DELIVERY_STATUS]: status,
    });
    // Update total size of notifications sent
    const messageSize = new TextEncoder().encode(JSON.stringify(messages)).length;
    notificationSize.inc({
        [NOTIFICATION_LABEL_DESTINATION]: destination,
    }, messageSize);
    // update notification delivery delay
    if (delay) {
        deliveryDelay.set({
            [NOTIFICATION_LABEL_DESTINATION]: destination,
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
                    'fail',
                    messages,
                    null,
                );
            }
            const delay = Date.now() - starTime;
            onNotificationSent(
                this._destinationConfig.resource,
                'success',
                messages,
                delay,
            );
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
