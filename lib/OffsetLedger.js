const assert = require('assert');

/**
 * @class OffsetLedger
 * @classdesc Keep track of processed Kafka messages' offsets
 *
 * Get knowledge of the next committable offset per topic/partition,
 * when messages are processed asynchronously and processing may end
 * out-of-order.
 */
class OffsetLedger {
    constructor() {
        this._ledger = {};
    }

    _getPartitionOffsets(topic, partition) {
        let topicOffsets = this._ledger[topic];
        if (!topicOffsets) {
            topicOffsets = {};
            this._ledger[topic] = topicOffsets;
        }
        let partitionOffsets = topicOffsets[partition];
        if (!partitionOffsets) {
            partitionOffsets = {
                processing: [],
                latestConsumed: null,
            };
            topicOffsets[partition] = partitionOffsets;
        }
        return partitionOffsets;
    }

    _getPartitionCommittableOffset(partitionOffsets) {
        // we can commit up to the lowest offset still being processed
        // since it means all lower offsets have already been
        // processed. If nothing is being processed, the latest
        // consumed message offset can be committed (so +1 for the
        // commit offset).

        if (partitionOffsets.processing.length !== 0) {
            return partitionOffsets.processing[0];
        }
        if (partitionOffsets.latestConsumed !== null) {
            return partitionOffsets.latestConsumed + 1;
        }
        return null;
    }

    /**
     * Function to be called as soon as a new message is received from
     * a Kafka topic and about to start being processed.
     *
     * @param {string} topic - topic name
     * @param {number} partition - partition number
     * @param {number} offset - offset of consumed message
     * @return {undefined}
     */
    onOffsetConsumed(topic, partition, offset) {
        // make sure offset is a positive number not to jeopardize
        // processing sanity
        assert(Number.isInteger(offset) && offset >= 0);

        const partitionOffsets = this._getPartitionOffsets(topic, partition);
        if (partitionOffsets.latestConsumed !== null &&
            offset !== partitionOffsets.latestConsumed + 1) {
            // replay situation (at-least-one semantics does not
            // prevent this from happening): get rid of all higher
            // offsets currently being processed, as they will be
            // reprocessed
            partitionOffsets.processing =
                partitionOffsets.processing.filter(pOff => pOff < offset);
        }
        partitionOffsets.processing.push(offset);
        partitionOffsets.latestConsumed = offset;
        return undefined;
    }

    /**
     * Function to be called when a message is completely processed.
     *
     * @param {string} topic - topic name
     * @param {number} partition - partition number
     * @param {number} offset - offset of processed message
     * @return {number} - highest committable offset for this
     * topic/partition (as returned by getCommittableOffset())
     */
    onOffsetProcessed(topic, partition, offset) {
        const partitionOffsets = this._getPartitionOffsets(topic, partition);
        partitionOffsets.processing =
            partitionOffsets.processing.filter(pOff => pOff !== offset);
        return this._getPartitionCommittableOffset(partitionOffsets);
    }

    /**
     * Get the highest committable offset for a topic/partition
     *
     * @param {string} topic - topic name
     * @param {number} partition - partition number
     * @param {number} offset - offset of processed message
     * @return {number} - highest committable offset for this topic/partition
     */
    getCommittableOffset(topic, partition) {
        const partitionOffsets = this._getPartitionOffsets(topic, partition);
        return this._getPartitionCommittableOffset(partitionOffsets);
    }

    /**
     * Get how many entries have been consumed but not yet fully
     * processed/committable
     *
     * @param {string} [topic] - topic name
     * @param {number} [partition] - partition number
     * @return {number} - number of consumed but not committable
     * entries for this topic/partition, or for this topic (if no
     * partition given), or for all topics and partitions (if none of
     * topic and partition is provided)
     */
    getProcessingCount(topic, partition) {
        if (topic && !this._ledger[topic]) {
            return 0;
        }
        if (topic && partition !== undefined &&
            !this._ledger[topic][partition]) {
            return 0;
        }
        let count = 0;
        const topics = topic ? [topic] : Object.keys(this._ledger);
        topics.forEach(t => {
            const partitions = partition !== undefined ?
                  [partition] : Object.keys(this._ledger[t]);
            partitions.forEach(p => {
                const partitionOffsets = this._ledger[t][p];
                count += partitionOffsets.processing.length;
            });
        });
        return count;
    }

    /**
     * Export the ledger in JSON format (useful for debugging)
     *
     * @return {string} a JSON-serialized representation of the
     * current state of the ledger
     */
    toString() {
        return JSON.stringify(this._ledger);
    }
}

module.exports = OffsetLedger;
