const assert = require('assert');

const QueueEntry = require('../../../../lib/models/QueueEntry');
const { replicationEntry } = require('../../../utils/kafkaEntries');

describe('ObjectQueueEntry', () => {
    describe('toRetryEntry', () => {
        it('Should not clear dataStoreVersionId when retrying', () => {
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            const dataStoreVersionId = entry.getReplicationSiteDataStoreVersionId('sf');
            const retryEntry = entry.toRetryEntry('sf');
            assert.strictEqual(retryEntry.getReplicationSiteDataStoreVersionId('sf'),
                dataStoreVersionId);
        });
        it('Should only include failed site details', () => {
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            const retryEntry = entry.toRetryEntry('sf');
            assert.strictEqual(retryEntry.getReplicationBackends().length, 1);
            assert.strictEqual(retryEntry.getReplicationBackends()[0].site, 'sf');
        });
    });
});
