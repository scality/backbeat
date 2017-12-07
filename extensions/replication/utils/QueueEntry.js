const { usersBucket } = require('arsenal').constants;

const ObjectQueueEntry = require('./ObjectQueueEntry');
const BucketQueueEntry = require('./BucketQueueEntry');

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

    getBucket() {
        return this.bucket;
    }

    setBucket(bucket) {
        this.bucket = bucket;
        return this;
    }

    getObjectKey() {
        return this.objectKey;
    }

    getObjectVersionedKey() {
        return this.objectVersionedKey;
    }

    isObjectMasterKey() {
        return isMasterKey(this.objectVersionedKey);
    }

    getEntryType() {
        return this.entryType;
    }


    getUserMetadata() {
        const metaHeaders = {};
        const data = this.getValue();
        Object.keys(data).forEach(key => {
            if (key.startsWith('x-amz-meta-')) {
                metaHeaders[key] = data[key];
            }
        });
        if (Object.keys(metaHeaders).length > 0) {
            return JSON.stringify(metaHeaders);
        }
        return undefined;
    }

    getLogInfo() {
        return {
            bucket: this.getBucket(),
            objectKey: this.getObjectKey(),
            versionId: this.getVersionId(),
            isDeleteMarker: this.getIsDeleteMarker(),
        };
    }

    toReplicaEntry() {
        const newEntry = this.clone();
        newEntry.setBucket(this.getReplicationTargetBucket());
        newEntry.setReplicationStatus('REPLICA');
        return newEntry;
    }

    toCompletedEntry() {
        const newEntry = this.clone();
        newEntry.setReplicationStatus('COMPLETED');
        return newEntry;
    }

    toFailedEntry() {
        const newEntry = this.clone();
        newEntry.setReplicationStatus('FAILED');
        return newEntry;
    }
}

module.exports = QueueEntry;
