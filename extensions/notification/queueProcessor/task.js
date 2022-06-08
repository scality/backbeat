'use strict'; // eslint-disable-line
const assert = require('assert');
const werelogs = require('werelogs');

const QueueProcessor = require('./QueueProcessor');
const config = require('../../../lib/Config');
const kafkaConfig = config.kafka;
const notifConfig = config.extensions.notification;
const mongoConfig = config.queuePopulator.mongo;

const log = new werelogs.Logger('Backbeat:NotificationProcessor:task');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

try {
    const destination = process.argv[2];
    assert(destination, 'task must be started with a destination as argument');
    const queueProcessor = new QueueProcessor(
        mongoConfig, kafkaConfig, notifConfig, destination);
    queueProcessor.start();
} catch (err) {
    log.error('error starting notification queue processor task', {
        method: 'notification.task.queueProcessor.start',
        error: err,
    });
}
