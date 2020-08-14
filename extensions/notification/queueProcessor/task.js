'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const QueueProcessor = require('./QueueProcessor');
const config = require('../../../lib/Config');
const kafkaConfig = config.kafka;
const notifConfig = config.extensions.notification;

const log = new werelogs.Logger('Backbeat:NotificationProcessor:task');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

try {
    // TODO: fetch the destination for the processor either via environment
    // variable or the configuration file. Also check if the id/resource of
    // the destination should be preserved in the environment or the config file
    const destinationType = undefined;
    const queueProcessor
        = new QueueProcessor(kafkaConfig, notifConfig, destinationType);
    // TODO: check if bucket notification configuration exists before starting
    // the processor. Setting up watcher here or in the processor, TBD.
    queueProcessor.start();
} catch (err) {
    log.error('error starting queue processor task', {
        method: 'task.queueProcessor.start',
        error: err,
    });
}
