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
    // get destination auth config from environment variables
    let destinationAuth = {
        type: process.env.TYPE,
        ssl: process.env.SSL === 'true',
        protocol: process.env.PROTOCOL,
        ca: process.env.CA,
        client: process.env.CLIENT,
        key: process.env.KEY,
        keyPassword: process.env.KEY_PASSWORD,
        keytab: process.env.KEYTAB,
        principal: process.env.PRINCIPAL,
        serviceName: process.env.SERVICE_NAME,
    };
    const isDestinationAuthEmpty = Object.values(destinationAuth)
        .every(x => !x);
    if (isDestinationAuthEmpty) {
        destinationAuth = null;
    }
    const queueProcessor = new QueueProcessor(
        mongoConfig, kafkaConfig, notifConfig, destination, destinationAuth);
    queueProcessor.start();
} catch (err) {
    log.error('error starting notification queue processor task', {
        method: 'notification.task.queueProcessor.start',
        error: err,
    });
}
