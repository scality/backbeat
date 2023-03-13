const config = require('../../lib/Config');
const locations = require('../../conf/locationConfig.json') || {};

const ActionQueueEntry = require('../../lib/models/ActionQueueEntry');
const ReplicationMetrics = require('./ReplicationMetrics');

let { dataMoverTopic } = config.extensions.replication;
const { coldStorageArchiveTopicPrefix } = config.extensions.lifecycle;
const { LifecycleMetrics } = require('../lifecycle/LifecycleMetrics');

class ReplicationAPI {
    /**
     * Create an action to copy an object's data to a new location.
     *
     * Pass the returned object to
     * {@link ReplicationAPI.sendDataMoverAction()} to queue the action
     * to the data mover service.
     *
     * @param {object} params - params object
     * @param {string} params.bucketName - bucket name
     * @param {string} params.objectKey - object key
     * @param {string} params.accountId - object's bucket's account id
     * @param {string} [params.versionId] - encoded version ID
     * @param {string} [params.eTag] - ETag of object
     * @param {string} [params.lastModified] - object last modification date
     * @param {string} params.toLocation - name of target location
     * @param {string} params.originLabel - label to mark origin of
     * request (for metrics accounting)
     * @param {string} params.fromLocation - source location name (for
     * metrics accounting)
     * @param {string} params.contentLength - content length (for
     * metrics accounting)
     * @param {string} [params.resultsTopic] - name of topic to get
     * result message (no result will be published if not provided)
     * @return {ActionQueueEntry} new action entry
     */
    static createCopyLocationAction(params) {
        const action = ActionQueueEntry.create('copyLocation');
        action
            .setAttribute('target', {
                accountId: params.accountId,
                owner: params.owner,
                bucket: params.bucketName,
                key: params.objectKey,
                version: params.versionId,
                eTag: params.eTag,
                attempt: params.attempt,
                lastModified: params.lastModified,
            })
            .setAttribute('toLocation', params.toLocation)
            .setAttribute('metrics', {
                origin: params.originLabel,
                fromLocation: params.fromLocation,
                contentLength: params.contentLength,
            });
        if (params.resultsTopic) {
            action.setResultsTopic(params.resultsTopic);
        }
        return action;
    }

    /**
     * Send an action to the data mover service
     *
     * @param {BackbeatProducer} producer - backbeat producer instance
     * @param {ActionQueueEntry} action - The action entry to send to
     * the data mover service
     * @param {Logger.newRequestLogger} log - logger object
     * @param {Function} cb - callback: cb(err)
     * @return {undefined}
     */
    static sendDataMoverAction(producer, action, log, cb) {
        const { accountId, bucket, key, version, eTag, attempt } = action.getAttribute('target');
        const { origin, fromLocation, contentLength } = action.getAttribute('metrics');
        const kafkaEntry = {
            key: `${bucket}/${key}`,
            message: action.toKafkaMessage(),
        };
        let topic = dataMoverTopic;
        const toLocation = action.getAttribute('toLocation');
        const locationConfig = locations[toLocation];
        if (!locationConfig) {
            const errorMsg = 'could not get destination location configuration';
            log.error(errorMsg, { method: 'ReplicationAPI.sendDataMoverAction' });
            return cb(new Error(errorMsg));
        }
        if (locationConfig.isCold) {
            topic = `${coldStorageArchiveTopicPrefix}${toLocation}`;
            const { reqId } = action.getContext();
            const message = {
                accountId,
                bucketName: bucket,
                objectKey: key,
                objectVersion: version,
                requestId: reqId,
                // TODO: BB-217 do not use contentLength from metrics
                size: contentLength,
                eTag,
                try: attempt,
            };
            kafkaEntry.message = JSON.stringify(message);
        }
        console.log('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ TRANSITION END!! => topic', topic);
        console.log('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ TRANSITION END!! => kafkaEntry', kafkaEntry);
        // return producer.sendToTopic(topic, [kafkaEntry], (err, reports) => {
        //     if (locationConfig.isCold) {
        //         LifecycleMetrics.onKafkaPublish(log, 'ColdStorageArchiveTopic', 'bucket', err, 1);
        //     }
        //     if (err) {
        //         log.error('could not send data mover action',
        //             Object.assign({
        //                 method: 'ReplicationAPI.sendDataMoverAction',
        //                 error: err,
        //             }, action.getLogInfo()));
        //         return cb(err);
        //     }
        //     log.debug('sent action to the data mover',
        //         Object.assign({
        //             method: 'ReplicationAPI.sendDataMoverAction',
        //         }, action.getLogInfo()));
        //     ReplicationMetrics.onReplicationQueued(
        //         origin, fromLocation, action.getAttribute('toLocation'),
        //         contentLength, reports[0].partition);
        //     return cb();
        // });
    }

    static getDataMoverTopic() {
        return dataMoverTopic;
    }

    static setDataMoverTopic(newTopicName) {
        dataMoverTopic = newTopicName;
    }
}

module.exports = ReplicationAPI;
