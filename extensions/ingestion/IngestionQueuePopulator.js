const { zenkoIDHeader } = require('arsenal').constants;

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const ObjectQueueEntry = require('../../lib/models/ObjectQueueEntry');

class IngestionQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.config = params.config;
        this._instanceId = params.instanceId;

        this._batch = [];
    }

    // called by _processLogEntry in lib/queuePopulator/LogReader.js
    filter(entry) {
        if (entry.type !== 'put' && entry.type !== 'del') {
            this.log.trace('skipping entry because not type put or del');
            return;
        }
        // Note that del entries at least have a bucket and key
        // and that bucket metadata entries at least have a bucket
        if (!entry.bucket) {
            this.log.trace('skipping entry because missing bucket name');
            return;
        }
        if (entry.value && this._filterValueOp(entry)) {
            return;
        }

        this.log.debug('publishing entry',
                       { entryBucket: entry.bucket, entryKey: entry.key });
        // entry.bucket === zenko target bucket name
        this.publish(entry.bucket,
                     `${entry.bucket}/${entry.key}`,
                     JSON.stringify(entry));

        this._incrementMetrics(entry.bucket);
    }

    _filterValueOp(entry) {
        const metadataVal = JSON.parse(entry.value);
        if (this._isBucketEntry(metadataVal)) {
            this.log.trace('skipping bucket entry');
            return true;
        }
        if (entry.type === 'put') {
            const queueEntry = new ObjectQueueEntry(entry.bucket,
                                                    entry.key,
                                                    metadataVal);
            const sanityCheckRes = queueEntry.checkSanity();
            if (sanityCheckRes) {
                this.log.trace('entry malformed', {
                    method: 'IngestionQueuePopulator.filter',
                    error: sanityCheckRes.message,
                    bucket: entry.bucket,
                    key: entry.key,
                    type: entry.type,
                });
                return true;
            }
            if (this._isRetroPropagationEntry(queueEntry)) {
                this.log.trace('skipping retro-propagated entry');
                return true;
            }
        }
        return false;
    }

    /**
     * Filter out bucket metadata entries from S3C Raft logs
     * @param {Object} metadataVal - entry metadata value
     * @return {Boolean} true if we should filter entry
     */
    _isBucketEntry(metadataVal) {
        // If `attributes` key exists in metadata, this is a nested bucket
        // metadata entry for s3c buckets
        if (metadataVal.mdBucketModelVersion ||
            metadataVal.attributes) {
            return true;
        }
        return false;
    }

    /**
     * Retro-propagation is where S3C ingestion will re-ingest an object whose
     * request originated from Zenko. Filter these entries indicated by user
     * metadata field defined by constants.zenkoIDHeader
     * @param {ObjectQueueEntry} entry - object queue entry instance
     * @return {Boolean} true if we should filter entry
     */
    _isRetroPropagationEntry(entry) {
        const userMD = entry.getUserMetadata();
        let existingIDHeader;
        if (userMD) {
            try {
                const metaHeaders = JSON.parse(userMD);
                existingIDHeader = metaHeaders[zenkoIDHeader];
            } catch (err) {
                this.log.trace('malformed user metadata', {
                    method: 'IngestionQueuePopulator.filter',
                    bucket: entry.bucket,
                    key: entry.key,
                    type: entry.type,
                });
                return true;
            }
            // if user metadata field of `constants.zenkoIDHeader`
            // exists and value is either 'zenko' (for delete markers)
            // or matches given hashed instance id
            if (existingIDHeader && (existingIDHeader === 'zenko' ||
                existingIDHeader === this._instanceId)) {
                this.log.trace('skipping retro-propagated entry');
                return true;
            }
        }
        return false;
    }

    publish(zenkoBucket, key, message) {
        const kafkaEntry = { key: encodeURIComponent(key), message };
        this.log.trace('queueing kafka entry to topic', {
            key: kafkaEntry.key,
            bucket: zenkoBucket,
        });
        this._batch.push(kafkaEntry);
    }

    getEntries() {
        const dupe = this._batch.slice();
        if (dupe.length > 0) {
            this._batch.splice(0, dupe.length);
        }
        return dupe;
    }

    /**
     * Get currently stored metrics for given bucket and reset its counter
     * @param {String} bucket - zenko bucket name
     * @return {Integer} metrics accumulated since last called
     */
    getAndResetMetrics(bucket) {
        const tempStore = this._metricsStore[bucket];
        if (tempStore === undefined) {
            return undefined;
        }
        this._metricsStore[bucket] = { ops: 0 };
        return tempStore;
    }

    /**
     * Set or accumulate metrics based on bucket
     * @param {String} bucket - Zenko bucket name
     * @return {undefined}
     */
    _incrementMetrics(bucket) {
        if (!this._metricsStore[bucket]) {
            this._metricsStore[bucket] = {
                ops: 1,
            };
        } else {
            this._metricsStore[bucket].ops++;
        }
    }
}

module.exports = IngestionQueuePopulator;
