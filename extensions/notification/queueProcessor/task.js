'use strict'; // eslint-disable-line
const assert = require('assert');
const werelogs = require('werelogs');

const QueueProcessor = require('./QueueProcessor');
const config = require('../../../lib/Config');
const kafkaConfig = config.kafka;
const notifConfig = config.extensions.notification;
const mongoConfig = config.queuePopulator.mongo;
const notificationDestinations = config.bucketNotificationDestinations;

const log = new werelogs.Logger('Backbeat:NotificationProcessor:task');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

try {
    const destination = process.argv[2];
    assert(destination, 'task must be started with a destination as argument');
    const destinationConfig
        = notificationDestinations.find(dest => dest.resource === destination);
    assert(destinationConfig, 'Invalid destination argument. Destination ' +
        'could not be found in destinations defined');
    const queueProcessor = new QueueProcessor(
        mongoConfig, kafkaConfig, notifConfig, destinationConfig, destination);
    queueProcessor.start();
} catch (err) {
    log.error('error starting notification queue processor task', {
        method: 'notification.task.queueProcessor.start',
        error: err,
    });
}
