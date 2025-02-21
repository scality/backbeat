const assert = require('assert');

const QueueEntry = require('../../../../lib/models/QueueEntry');
const { replicationEntry } = require('../../../utils/kafkaEntries');
const DeleteOpQueueEntry = require('../../../../lib/models/DeleteOpQueueEntry');

describe('ObjectQueueEntry', () => {
    describe('toRetryEntry', () => {
        it('should not clear dataStoreVersionId when retrying', () => {
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            const dataStoreVersionId = entry.getReplicationSiteDataStoreVersionId('sf');
            const retryEntry = entry.toRetryEntry('sf');
            assert.strictEqual(retryEntry.getReplicationSiteDataStoreVersionId('sf'),
                dataStoreVersionId);
        });
        it('should only include failed site details', () => {
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            const retryEntry = entry.toRetryEntry('sf');
            assert.strictEqual(retryEntry.getReplicationBackends().length, 1);
            assert.strictEqual(retryEntry.getReplicationBackends()[0].site, 'sf');
        });
    });
    describe('createFromKafkaEntry', () => {
        it('should create a DeleteOpQueueEntry without overhead fields', () => {
            const entry = QueueEntry.createFromKafkaEntry({
                value: JSON.stringify({
                    type: 'del',
                    bucket: 'bucket',
                    key: 'key',
                }),
            });
            assert(entry instanceof DeleteOpQueueEntry);
            assert.strictEqual(entry.getBucket(), 'bucket');
            assert.strictEqual(entry.getKey(), 'key');
        });
        it('should create a DeleteOpQueueEntry with overhead fields', () => {
            const entry = QueueEntry.createFromKafkaEntry({
                value: JSON.stringify({
                    type: 'del',
                    bucket: 'bucket',
                    key: 'key',
                    overheadFields: JSON.stringify({ foo: 'bar' }),
                }),
            });
            assert(entry instanceof DeleteOpQueueEntry);
            assert.strictEqual(entry.getBucket(), 'bucket');
            assert.strictEqual(entry.getKey(), 'key');
            assert.deepStrictEqual(entry.getOverheadField('foo'), 'bar');
        });
    });
});
