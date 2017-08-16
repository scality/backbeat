'use strict'; // eslint-disable-line

const assert = require('assert');

const QueueEntry =
          require('../../../extensions/replication/utils/QueueEntry');
const kafkaEntry = require('../../utils/kafkaEntry');

describe('QueueEntry helper class', () => {
    describe('built from Kafka queue entry', () => {
        it('should parse a well-formed kafka entry', () => {
            const entry = QueueEntry.createFromKafkaEntry(kafkaEntry);
            assert.strictEqual(entry.error, undefined);
            assert.strictEqual(entry.getBucket(),
                               'queue-populator-test-bucket');
            assert.strictEqual(entry.getObjectKey(), 'hosts');
            assert.strictEqual(entry.getVersionId(),
                               '98500086134471999999RG001  0');
            assert.strictEqual(
                entry.getEncodedVersionId(),
                '39383530303038363133343437313939393939395247303031202030');
            assert.strictEqual(entry.getContentLength(), 542);
            assert.strictEqual(entry.getContentMD5(),
                               '01064f35c238bd2b785e34508c3d27f4');
            assert.strictEqual(entry.getReplicationStatus(), 'PENDING');
            const repContent = entry.getReplicationContent();
            assert.deepStrictEqual(repContent, ['DATA', 'METADATA']);
            const destBucket = entry.getReplicationDestBucket();
            assert.deepStrictEqual(destBucket, 'dummy-dest-bucket');
        });

        it('should convert a kafka entry\'s replication status', () => {
            const entry = QueueEntry.createFromKafkaEntry(kafkaEntry);
            assert.strictEqual(entry.error, undefined);

            const replica = entry.toReplicaEntry();
            assert.strictEqual(replica.getReplicationStatus(), 'REPLICA');

            const completed = entry.toCompletedEntry();
            assert.strictEqual(completed.getReplicationStatus(), 'COMPLETED');
        });
    });
});
