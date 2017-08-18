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

            const replica = entry.toReplicaEntry();
            assert.strictEqual(replica.getReplicationStatus(), 'REPLICA');

            const completed = entry.toCompletedEntry();
            assert.strictEqual(completed.getReplicationStatus(), 'COMPLETED');
        });
    });

    describe('QueueEntry.getReducedLocations helper method', () => {
        it('should not alter an array when each part has only one element',
        () => {
            const entry = QueueEntry.createFromKafkaEntry(kafkaEntry);
            const locations = [
                {
                    key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                    size: 1,
                    start: 0,
                    dataStoreName: 'file',
                    dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
                },
                {
                    key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                    size: 3,
                    start: 2,
                    dataStoreName: 'file',
                    dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
                },
            ];
            entry.setLocation(locations);
            assert.deepStrictEqual(entry.getReducedLocations(), locations);
        });

        it('should reduce an array when first part is > 1 element', () => {
            const entry = QueueEntry.createFromKafkaEntry(kafkaEntry);
            entry.setLocation([
                {
                    key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                    size: 1,
                    start: 0,
                    dataStoreName: 'file',
                    dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
                },
                {
                    key: 'deebfb287cfcee1d137b0136562d2d776ba491e1',
                    size: 1,
                    start: 1,
                    dataStoreName: 'file',
                    dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
                },
                {
                    key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                    size: 3,
                    start: 2,
                    dataStoreName: 'file',
                    dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
                },
            ]);
            assert.deepStrictEqual(entry.getReducedLocations(), [
                {
                    key: 'deebfb287cfcee1d137b0136562d2d776ba491e1',
                    size: 2,
                    start: 1,
                    dataStoreName: 'file',
                    dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
                },
                {
                    key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                    size: 3,
                    start: 2,
                    dataStoreName: 'file',
                    dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
                },
            ]);
        });

        it('should reduce an array when second part is > 1 element', () => {
            const entry = QueueEntry.createFromKafkaEntry(kafkaEntry);
            entry.setLocation([
                {
                    key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                    size: 1,
                    start: 0,
                    dataStoreName: 'file',
                    dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
                },
                {
                    key: 'deebfb287cfcee1d137b0136562d2d776ba491e1',
                    size: 1,
                    start: 1,
                    dataStoreName: 'file',
                    dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
                },
                {
                    key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                    size: 3,
                    start: 2,
                    dataStoreName: 'file',
                    dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
                },
            ]);
            assert.deepStrictEqual(entry.getReducedLocations(), [
                {
                    key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                    size: 1,
                    start: 0,
                    dataStoreName: 'file',
                    dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
                },
                {
                    key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                    size: 4,
                    start: 2,
                    dataStoreName: 'file',
                    dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
                },
            ]);
        });

        it('should reduce an array when multiple parts are > 1 element', () => {
            const entry = QueueEntry.createFromKafkaEntry(kafkaEntry);
            entry.setLocation([
                {
                    key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                    size: 1,
                    start: 0,
                    dataStoreName: 'file',
                    dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
                },
                {
                    key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                    size: 2,
                    start: 1,
                    dataStoreName: 'file',
                    dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
                },
                {
                    key: 'deebfb287cfcee1d137b0136562d2d776ba491e1',
                    size: 1,
                    start: 3,
                    dataStoreName: 'file',
                    dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
                },
                {
                    key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                    size: 3,
                    start: 4,
                    dataStoreName: 'file',
                    dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
                },
            ]);
            assert.deepStrictEqual(entry.getReducedLocations(), [
                {
                    key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                    size: 3,
                    start: 1,
                    dataStoreName: 'file',
                    dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
                },
                {
                    key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                    size: 4,
                    start: 4,
                    dataStoreName: 'file',
                    dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
                },
            ]);
        });
    });
});
