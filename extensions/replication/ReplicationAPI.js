const config = require('../../conf/Config');

const ActionQueueEntry = require('../../lib/models/ActionQueueEntry');
const ReplicationMetrics = require('./ReplicationMetrics');

let { dataMoverTopic } = config.extensions.replication;

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
                bucket: params.bucketName,
                key: params.objectKey,
                version: params.versionId,
                eTag: params.eTag,
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
        const { bucket, key } = action.getAttribute('target');
        const kafkaEntries = [{
            key: `${bucket}/${key}`,
            message: action.toKafkaMessage(),
        }];
        producer.sendToTopic(dataMoverTopic, kafkaEntries, (err, reports) => {
            if (err) {
                log.error('could not send data mover action',
                    Object.assign({
                        method: 'ReplicationAPI.sendDataMoverAction',
                        error: err,
                    }, action.getLogInfo()));
                return cb(err);
            }
            log.debug('sent action to the data mover',
                Object.assign({
                    method: 'ReplicationAPI.sendDataMoverAction',
                }, action.getLogInfo()));
            const { origin, fromLocation, contentLength } = action.getAttribute('metrics');
            ReplicationMetrics.onReplicationQueued(
                origin, fromLocation, action.getAttribute('toLocation'),
                contentLength, reports[0].partition
            );
            return cb();
        });
    }

    static getDataMoverTopic() {
        return dataMoverTopic;
    }

    static setDataMoverTopic(newTopicName) {
        dataMoverTopic = newTopicName;
    }
}

module.exports = ReplicationAPI;
