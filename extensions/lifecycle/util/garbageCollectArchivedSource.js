'use strict';

const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { LifecycleMetrics } = require('../LifecycleMetrics');

/**
 * Ask the garbage collector to release the hot data of an archived object. On completion, it
 * updates the object metadata to point to the cold location, which ends the transition.
 *
 * @param {Object} gcProducer - garbage collector producer
 * @param {Object} target - object to collect
 * @param {string} target.bucketName - bucket name
 * @param {string} target.objectKey - object key
 * @param {string} [target.objectVersion] - object version id
 * @param {string} [target.accountId] - account id
 * @param {string} [target.owner] - owner canonical id, used to resolve the account id if missing
 * @param {string} oldLocation - hot location holding the data
 * @param {string} newLocation - cold location the object was archived to
 * @param {Logger} log - request logger
 * @return {undefined}
 */
function garbageCollectArchivedSource(gcProducer, target, oldLocation, newLocation, log) {
    const { bucketName, objectKey, objectVersion, accountId, owner } = target;
    const gcEntry = ActionQueueEntry.create('deleteArchivedSourceData')
        .addContext({
            origin: 'lifecycle',
            ruleType: 'archive',
            reqId: log.getSerializedUids(),
            bucketName,
            objectKey,
            versionId: objectVersion,
        })
        .setAttribute('serviceName', 'lifecycle-transition')
        .setAttribute('target.oldLocation', oldLocation)
        .setAttribute('target.newLocation', newLocation)
        .setAttribute('target.bucket', bucketName)
        .setAttribute('target.key', objectKey)
        .setAttribute('target.version', objectVersion)
        .setAttribute('target.accountId', accountId)
        .setAttribute('target.owner', owner);
    gcProducer.publishActionEntry(gcEntry, err => {
        LifecycleMetrics.onKafkaPublish(log, 'GCTopic', 'archive', err, 1);
    });
}

module.exports = { garbageCollectArchivedSource };
