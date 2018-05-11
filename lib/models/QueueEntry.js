const { usersBucket } = require('arsenal').constants;

const ObjectQueueEntry =
    require('../../extensions/utils/ObjectQueueEntry');
const BucketQueueEntry =
    require('../../extensions/utils/BucketQueueEntry');

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
            let entry;
            if (record.bucket === usersBucket) {
                entry = new BucketQueueEntry(record.key);
            } else {
                const mdObj = JSON.parse(record.value);
                entry = new ObjectQueueEntry(record.bucket, record.key, mdObj);
                entry.setSite(record.site);
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
