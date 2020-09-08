const { versioning } = require('arsenal');
const versionIdUtils = versioning.VersionID;

const constants = require('../constants');

/**
 * Add attributes to an internal notification message,
 * with its log entry counterpart.
 * @param  {Object} logEntry - log entry that generated the message
 * @param  {Object} message - initial message generated
 * @return {Object} updated message object
 */
function addLogAttributes(logEntry, message) {
    const { eventMessageProperty } = constants;
    const msg = {};
    Object.entries(eventMessageProperty).forEach(([key, value]) => {
        let val = logEntry[value];
        if (typeof val === 'number') {
            val = val.toString();
        }
        msg[key] = val || null;
    });
    return Object.assign(msg, message);
}

/**
 * Transforms an event object to specification.
 * @param  {Object} event - event object
 * @return {Object} updated event object
 */
function transformToSpec(event) {
    const encodedVersionId
        = event.versionId ? versionIdUtils.encode(event.versionId) : null;
    const message = {
        Records: [
            {
                eventVersion: constants.eventVersion,
                eventSource: constants.eventSource,
                awsRegion: event.region,
                eventTime: event.dateTime,
                eventName: event.eventType,
                userIdentity: {
                    principalId: null,
                },
                requestParameters: {
                    sourceIPAddress: null,
                },
                responseElements: {
                    'x-amz-request-id': null,
                    'x-amz-id-2': null,
                },
                s3: {
                    s3SchemaVersion: constants.eventS3SchemaVersion,
                    configurationId: event.configurationId,
                    bucket: {
                        name: event.bucket,
                        ownerIdentity: {
                            principalId: null,
                        },
                        arn: null,
                    },
                    object: {
                        key: event.key,
                        size: event.size,
                        eTag: null,
                        versionId: encodedVersionId,
                        sequencer: null,
                    },
                },
            },
        ],
    };
    return message;
}

module.exports = {
    addLogAttributes,
    transformToSpec,
};
