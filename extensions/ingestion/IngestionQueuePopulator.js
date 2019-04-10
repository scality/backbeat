const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const { isMasterKey } = require('arsenal/lib/versioning/Version');
const ObjectQueueEntry = require('../../lib/models/ObjectQueueEntry');

class IngestionQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.config = params.config;
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
        if (entry.value) {
            const metadataVal = JSON.parse(entry.value);
            // Filter out bucket metadata entries
            // If `attributes` key exists in metadata, this is a nested bucket
            // metadata entry for s3c buckets
            if (metadataVal.mdBucketModelVersion ||
                metadataVal.attributes) {
                return;
            }
            // Filter out any master key object entries
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
                    return;
                }
                // Filter if master key and is not a single null version
                // with no internal version id set.
                // This null case will only apply for a previously
                // non-versioned bucket that has now become versioned, and
                // the user ingests these null objects.
                // The `isNull` case is undefined for these entries.
                if (isMasterKey(queueEntry.getObjectVersionedKey()) &&
                    queueEntry.getVersionId() !== undefined) {
                    this.log.trace('skipping master key entry');
                    return;
                }
            }
        }

        this.log.debug('publishing entry',
                       { entryBucket: entry.bucket, entryKey: entry.key });
        this.publish(entry.id,
                     `${entry.bucket}/${entry.key}`,
                     JSON.stringify(entry));

        this._incrementMetrics(entry.bucket);
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
