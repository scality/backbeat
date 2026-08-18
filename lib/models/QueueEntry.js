const { usersBucket } = require('arsenal').constants;

const ObjectQueueEntry = require('./ObjectQueueEntry');
const BucketQueueEntry = require('./BucketQueueEntry');
const BucketMdQueueEntry = require('./BucketMdQueueEntry');
const DeleteOpQueueEntry = require('./DeleteOpQueueEntry');

/**
 * Decode a nested field of a kafka entry, which producers may send either
 * as a JSON string or as an already-decoded object.
 *
 * @param {string|object} field - JSON string or decoded object
 * @return {object} the decoded field
 * @throws {Error} if the field is neither a JSON string nor an object; the
 *   caller turns this into a "malformed JSON in kafka entry" error
 */
function decodeField(field) {
    if (typeof field === 'string') {
        return JSON.parse(field);
    }
    if (typeof field === 'object') {
        return field;
    }
    throw new Error(
        `expected a JSON string or an object, got ${typeof field}`);
}

class QueueEntry {

    /**
     * factory method that creates the proper sub-class instance
     * depending on the type of kafka entry (object/bucket/whatever)
     *
     * @param {Object} kafkaEntry - entry as read from Kafka queue

     * @return {Object} - an object which inherits from
     *   {@link QueueEntry} base class
     */
    static createFromKafkaEntry(kafkaEntry) {
        try {
            const record = JSON.parse(kafkaEntry.value);
            if (record.bootstrapId) {
                return { error: 'bootstrap entry' };
            }
            if (record.canary) {
                return { skip: 'skip canary entry' };
            }
            let entry;
            if (record.type === 'del') {
                const overheadFields = record.overheadFields && decodeField(record.overheadFields);
                entry = new DeleteOpQueueEntry(record.bucket, record.key, overheadFields);
            } else if (record.bucket === usersBucket) {
                // BucketQueueEntry class just handles puts of keys
                // to usersBucket
                entry = new BucketQueueEntry(record.key, record.value);
            } else if (record.value) {
                const metadataVal = decodeField(record.value);
                if (metadataVal.mdBucketModelVersion) {
                    // it's bucket metadata
                    entry = new BucketMdQueueEntry(record.key, metadataVal);
                } else if (metadataVal.attributes) {
                    // S3 Connector bucket metadata is within
                    // an attributes object
                    const nestedVal = JSON.parse(metadataVal.attributes);
                    entry = new BucketMdQueueEntry(nestedVal.name, nestedVal);
                } else {
                    // it's object metadata

                    // TODO: consider having a separate elseif/entry type
                    // for mpu parts since those entries do not have
                    // full object metadata
                    entry = new ObjectQueueEntry(record.bucket, record.key,
                                                 metadataVal, {
                                                     workflowId: record.workflowId,
                                                     workflowVersion: record.workflowVersion,
                                                     nodeId: record.nodeId,
                                                     uniqueId: record.uniqueId,
                                                     ignore: record.ignore,
                                                 });
                    entry.setReplicationBackend(record);
                    entry.setReplayCount(record.replayCount);
                    entry.setAccountId(record.accountId);
                }
            } else {
                return { error: 'unknown kafka entry format' };
            }
            const err = entry.checkSanity();
            if (err) {
                return { error: err };
            }
            return entry;
        } catch (err) {
            return { error: { message: 'malformed JSON in kafka entry',
                              description: err.message } };
        }
    }
}

module.exports = QueueEntry;
