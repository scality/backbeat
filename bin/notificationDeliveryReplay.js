'use strict';

/**
 * Cutover replay: drains whatever the old per-destination queue
 * processors left undelivered in the internal notification topics into
 * the delivery topic, as addressed records.
 *
 * Run it once, with the queue populator and every notification queue
 * processor stopped, before starting the delivery worker pool. It exits
 * 0 once every destination is drained, non-zero otherwise. It writes no
 * consumer group offset, so a failed run can simply be rerun, and going
 * back to the old pipeline stays possible.
 *
 * Usage: node bin/notificationDeliveryReplay.js [replayId]
 * The optional replayId only names the throwaway consumer group.
 */

const async = require('async');
const werelogs = require('werelogs');

const config = require('../lib/Config');
const kafkaConfig = config.kafka;
const notifConfig = config.extensions.notification;
const mongoConfig = config.queuePopulator.mongo;
const zkConfig = config.zookeeper;
const DeliveryTopicDrainer =
    require('../extensions/notification/deliveryWorker/DeliveryTopicDrainer');

const log = new werelogs.Logger('Backbeat:Notification:DeliveryReplay');

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

const replayId = process.argv[2] || String(Date.now());

const drainer = new DeliveryTopicDrainer({
    kafkaConfig,
    mongoConfig,
    zkConfig,
    notifConfig,
    replayId,
    logger: log,
});

async.waterfall([
    next => drainer.start(err => next(err)),
    next => drainer.run(next),
], (err, totals) => {
    if (err) {
        log.error('notification delivery replay failed', {
            method: 'notificationDeliveryReplay',
            error: err.message,
        });
        return drainer.stop(() => process.exit(1));
    }
    log.info('notification delivery replay done', totals);
    return drainer.stop(() => process.exit(0));
});

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    drainer.stop(() => process.exit(1));
});
