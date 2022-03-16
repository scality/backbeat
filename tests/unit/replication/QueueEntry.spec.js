'use strict'; // eslint-disable-line

const assert = require('assert');

const QueueEntry =
          require('../../../lib/models/QueueEntry');
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
            assert.strictEqual(entry.getContentMd5(),
                               '01064f35c238bd2b785e34508c3d27f4');
            assert.strictEqual(entry.getReplicationStatus(), 'PENDING');
            const repContent = entry.getReplicationContent();
            assert.deepStrictEqual(repContent, ['DATA', 'METADATA']);
            const destBucket = entry.getReplicationTargetBucket();
            assert.deepStrictEqual(destBucket, 'dummy-dest-bucket');
        });

        it('should convert a kafka entry\'s replication status', () => {
            const entry = QueueEntry.createFromKafkaEntry(kafkaEntry);
            assert.strictEqual(entry.error, undefined);

            // If one site is a REPLICA, the global status should be REPLICA
            const replica = entry.toReplicaEntry('sf');
            assert.strictEqual(replica.getReplicationSiteStatus('sf'),
                'REPLICA');
            assert.strictEqual(
                replica.getReplicationSiteStatus('replicationaws'),
                'PENDING');
            assert.strictEqual(replica.getReplicationStatus(), 'REPLICA');

            // If one site is FAILED, the global status should be FAILED
            const failed = entry.toFailedEntry('sf');
            assert.strictEqual(failed.getReplicationSiteStatus('sf'),
                'FAILED');
            assert.strictEqual(
                replica.getReplicationSiteStatus('replicationaws'),
                'PENDING');
            assert.strictEqual(failed.getReplicationStatus(), 'FAILED');

            // If one site is still PENDING, the global status should be
            // PROCESSING even though one has completed
            const completed = entry.toCompletedEntry('sf');
            assert.strictEqual(completed.getReplicationSiteStatus('sf'),
                'COMPLETED');
            assert.strictEqual(
                completed.getReplicationSiteStatus('replicationaws'),
                'PENDING');
            assert.strictEqual(completed.getReplicationStatus(), 'PROCESSING');

            // If all sites are COMPLETED, the global status should be COMPLETED
            const completed1 = entry.toCompletedEntry('sf');
            const completed2 = entry.toCompletedEntry('replicationaws');
            assert.strictEqual(completed2
                .getReplicationSiteStatus('replicationaws'),
                'COMPLETED');
            assert.strictEqual(completed1.getReplicationSiteStatus('sf'),
                'COMPLETED');
            assert.strictEqual(completed1.getReplicationStatus(), 'COMPLETED');
        });
    });
});
