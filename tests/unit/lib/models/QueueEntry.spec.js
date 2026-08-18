'use strict';

const assert = require('assert');

const QueueEntry = require('../../../../lib/models/QueueEntry');
const { replicationEntry } = require('../../../utils/kafkaEntries');

describe('QueueEntry helper class', () => {
    describe('built from Kafka queue entry', () => {
        it('should parse a well-formed kafka entry', () => {
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
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
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            assert.strictEqual(entry.error, undefined);

            // If one site is a REPLICA, the global status should be REPLICA
            const replica = entry.toReplicaEntry({ site: 'sf' });
            assert.strictEqual(replica.getReplicationSiteStatus({ site: 'sf' }),
                'REPLICA');
            assert.strictEqual(
                replica.getReplicationSiteStatus({ site: 'replicationaws' }),
                'PENDING');
            assert.strictEqual(replica.getReplicationStatus(), 'REPLICA');

            // If one site is FAILED, the global status should be FAILED
            const failed = entry.toFailedEntry({ site: 'sf' });
            assert.strictEqual(failed.getReplicationSiteStatus({ site: 'sf' }),
                'FAILED');
            assert.strictEqual(
                replica.getReplicationSiteStatus({ site: 'replicationaws' }),
                'PENDING');
            assert.strictEqual(failed.getReplicationStatus(), 'FAILED');

            // If one site is still PENDING, the global status should be
            // PROCESSING even though one has completed
            const completed = entry.toCompletedEntry({ site: 'sf' });
            assert.strictEqual(completed.getReplicationSiteStatus({ site: 'sf' }),
                'COMPLETED');
            assert.strictEqual(
                completed.getReplicationSiteStatus({ site: 'replicationaws' }),
                'PENDING');
            assert.strictEqual(completed.getReplicationStatus(), 'PROCESSING');

            // If all sites are COMPLETED, the global status should be COMPLETED
            const completed1 = entry.toCompletedEntry({ site: 'sf' });
            const completed2 = entry.toCompletedEntry({ site: 'replicationaws' });
            assert.strictEqual(completed2
                .getReplicationSiteStatus({ site: 'replicationaws' }),
                'COMPLETED');
            assert.strictEqual(completed1.getReplicationSiteStatus({ site: 'sf' }),
                'COMPLETED');
            assert.strictEqual(completed1.getReplicationStatus(), 'COMPLETED');
        });
    });
});
