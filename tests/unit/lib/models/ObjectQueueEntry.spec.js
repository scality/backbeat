const assert = require('assert');

const QueueEntry = require('../../../../lib/models/QueueEntry');
const ObjectQueueEntry = require('../../../../lib/models/ObjectQueueEntry');
const { replicationEntry } = require('../../../utils/kafkaEntries');
const DeleteOpQueueEntry = require('../../../../lib/models/DeleteOpQueueEntry');

const TOP_LEVEL_BUCKET = 'legacy-dest';
const TOP_LEVEL_DEST = `arn:aws:s3:::${TOP_LEVEL_BUCKET}`;
const TOP_LEVEL_ROLE = 'arn:aws:iam::111:role/src,arn:aws:iam::222:role/dst';

function _makeEntryWithBackends(backends, topLevel = {}) {
    return new ObjectQueueEntry(
        'bucket',
        'key', {
        'md-model-version': 2,
        'replicationInfo': Object.assign({
            status: 'PENDING',
            content: ['DATA', 'METADATA'],
            destination: TOP_LEVEL_DEST,
            role: TOP_LEVEL_ROLE,
            backends,
        }, topLevel),
    });
}

describe('ObjectQueueEntry', () => {
    describe('per-site replication getters', () => {
        it('falls back to top-level destination when backend has none', () => {
            const entry = _makeEntryWithBackends([
                {
                    site: 'siteA',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                },
            ]);

            assert.strictEqual(
                entry.getReplicationTargetBucket({ site: 'siteA' }),
                TOP_LEVEL_BUCKET,
            );
        });

        it('returns per-backend destination when present', () => {
            const entry = _makeEntryWithBackends([
                {
                    site: 'siteA',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::222:role/dst',
                },
                {
                    site: 'siteB',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-b',
                    role: 'arn:aws:iam::333:role/dst',
                },
            ]);

            assert.strictEqual(entry.getReplicationTargetBucket({ site: 'siteA' }), 'bucket-a');
            assert.strictEqual(entry.getReplicationTargetBucket({ site: 'siteB' }), 'bucket-b');
        });

        it('falls back to top-level role string when backend has none', () => {
            const entry = _makeEntryWithBackends([
                {
                    site: 'siteA',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                },
            ]);

            assert.strictEqual(
                entry.getReplicationRoles({ site: 'siteA' }),
                TOP_LEVEL_ROLE,
            );
        });

        it('composes source role + per-backend destination role', () => {
            const entry = _makeEntryWithBackends([
                {
                    site: 'siteA',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::222:role/dst',
                },
                {
                    site: 'siteB',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-b',
                    role: 'arn:aws:iam::333:role/dst',
                },
            ]);

            assert.strictEqual(
                entry.getReplicationRoles({ site: 'siteA' }),
                'arn:aws:iam::111:role/src,arn:aws:iam::222:role/dst',
            );
            assert.strictEqual(
                entry.getReplicationRoles({ site: 'siteB' }),
                'arn:aws:iam::111:role/src,arn:aws:iam::333:role/dst',
            );
        });

        it('no-arg getters return top-level values verbatim', () => {
            const entry = _makeEntryWithBackends([
                {
                    site: 'siteA',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::222:role/dst'
                },
            ]);

            assert.strictEqual(entry.getReplicationTargetBucket(), TOP_LEVEL_BUCKET);
            assert.strictEqual(entry.getReplicationRoles(), TOP_LEVEL_ROLE);
        });

        it('toReplicaEntry stamps per-site destination bucket', () => {
            const entry = _makeEntryWithBackends([
                {
                    site: 'siteA',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::222:role/dst',
                },
                {
                    site: 'siteB',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-b',
                    role: 'arn:aws:iam::333:role/dst',
                },
            ]);

            const replicaA = entry.toReplicaEntry({ site: 'siteA' });
            const replicaB = entry.toReplicaEntry({ site: 'siteB' });
            assert.strictEqual(replicaA.getBucket(), 'bucket-a');
            assert.strictEqual(replicaB.getBucket(), 'bucket-b');
        });
    });

    describe('same-site backend disambiguation', () => {
        const _makeMultiDest = () => _makeEntryWithBackends([
            {
                site: 'siteA',
                status: 'PENDING',
                dataStoreVersionId: '',
                destination: 'arn:aws:s3:::bucket-a',
                role: 'arn:aws:iam::222:role/dst',
            },
            {
                site: 'siteA',
                status: 'PENDING',
                dataStoreVersionId: '',
                destination: 'arn:aws:s3:::bucket-b',
                role: 'arn:aws:iam::333:role/dst',
            },
        ]);

        it('site-only lookup returns first match (legacy behaviour)', () => {
            const entry = _makeMultiDest();

            assert.strictEqual(entry.getReplicationTargetBucket({ site: 'siteA' }), 'bucket-a');
        });

        it('site + destination targets the right backend', () => {
            const entry = _makeMultiDest();
            const key = { site: 'siteA', destination: 'arn:aws:s3:::bucket-b' };

            assert.strictEqual(entry.getReplicationTargetBucket(key), 'bucket-b');
            assert.strictEqual(entry.getReplicationRoles(key), 'arn:aws:iam::111:role/src,arn:aws:iam::333:role/dst');
            assert.strictEqual(entry.getReplicationSiteStatus(key), 'PENDING');
        });

        it('site + destination + role targets the right backend', () => {
            const entry = _makeEntryWithBackends([
                {
                    site: 'siteA',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::shared',
                    role: 'arn:aws:iam::222:role/dst',
                },
                {
                    site: 'siteA',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::shared',
                    role: 'arn:aws:iam::333:role/dst',
                },
            ]);

            assert.strictEqual(
                entry.getReplicationRoles({
                    site: 'siteA',
                    destination: 'arn:aws:s3:::shared',
                    role: 'arn:aws:iam::333:role/dst',
                }),
                'arn:aws:iam::111:role/src,arn:aws:iam::333:role/dst',
            );
        });

        it('setReplicationSiteStatus updates the right backend', () => {
            const entry = _makeMultiDest();
            entry.setReplicationSiteStatus({
                site: 'siteA',
                destination: 'arn:aws:s3:::bucket-b',
            }, 'COMPLETED');
            const backends = entry.getReplicationBackends();

            assert.strictEqual(backends[0].status, 'PENDING');
            assert.strictEqual(backends[1].status, 'COMPLETED');
        });

        it('toCompletedEntry only marks the targeted backend', () => {
            const entry = _makeMultiDest();
            const completed = entry.toCompletedEntry({ site: 'siteA', destination: 'arn:aws:s3:::bucket-b' });
            const backends = completed.getReplicationBackends();

            assert.strictEqual(backends[0].status, 'PENDING');
            assert.strictEqual(backends[1].status, 'COMPLETED');
        });

        it('toFailedEntry only marks the targeted backend', () => {
            const entry = _makeMultiDest();
            const failed = entry.toFailedEntry({ site: 'siteA', destination: 'arn:aws:s3:::bucket-b' });
            const backends = failed.getReplicationBackends();

            assert.strictEqual(backends[0].status, 'PENDING');
            assert.strictEqual(backends[1].status, 'FAILED');
        });

        it('toPendingEntry only marks the targeted backend', () => {
            const entry = _makeMultiDest();
            entry.setReplicationSiteStatus({
                site: 'siteA',
                destination: 'arn:aws:s3:::bucket-b'
            }, 'FAILED');
            const pending = entry.toPendingEntry({ site: 'siteA', destination: 'arn:aws:s3:::bucket-b' });
            const backends = pending.getReplicationBackends();

            assert.strictEqual(backends[0].status, 'PENDING');
            assert.strictEqual(backends[1].status, 'PENDING');
        });

        it('set/getReplicationSiteDataStoreVersionId targets the right backend', () => {
            const entry = _makeMultiDest();
            entry.setReplicationSiteDataStoreVersionId({
                site: 'siteA',
                destination: 'arn:aws:s3:::bucket-b'
            }, 'v-b');
            const backends = entry.getReplicationBackends();

            assert.strictEqual(backends[0].dataStoreVersionId, '');
            assert.strictEqual(backends[1].dataStoreVersionId, 'v-b');
            assert.strictEqual(
                entry.getReplicationSiteDataStoreVersionId({ site: 'siteA', destination: 'arn:aws:s3:::bucket-b' }),
                'v-b',
            );
        });

        it('getReplicationSiteStatus returns undefined when no backend matches', () => {
            const entry = _makeMultiDest();

            assert.strictEqual(entry.getReplicationSiteStatus({ site: 'siteZ' }), undefined);
            assert.strictEqual(
                entry.getReplicationSiteStatus({ site: 'siteA', destination: 'arn:aws:s3:::missing' }),
                undefined,
            );
        });

        it('getReplicationBackend returns the per-task identity', () => {
            const entry = _makeMultiDest().setReplicationBackend({
                site: 'siteA',
                destination: 'arn:aws:s3:::bucket-b',
                role: 'arn:aws:iam::333:role/dst',
            });

            assert.deepStrictEqual(entry.getReplicationBackend(), {
                site: 'siteA',
                destination: 'arn:aws:s3:::bucket-b',
                role: 'arn:aws:iam::333:role/dst',
            });
        });

        it('toReplicaEntry stamps the right destination bucket', () => {
            const entry = _makeMultiDest();
            const replica = entry.toReplicaEntry({ site: 'siteA', destination: 'arn:aws:s3:::bucket-b' });

            assert.strictEqual(replica.getBucket(), 'bucket-b');
        });

        it('toRetryEntry keeps only the targeted backend', () => {
            const entry = _makeMultiDest();
            const retry = entry.toRetryEntry({ site: 'siteA', destination: 'arn:aws:s3:::bucket-b' });
            const backends = retry.getReplicationBackends();

            assert.strictEqual(backends.length, 1);
            assert.strictEqual(backends[0].destination, 'arn:aws:s3:::bucket-b');
            assert.strictEqual(backends[0].status, 'PENDING');
        });

        it('toKafkaEntry serialises site + destination + role into payload', () => {
            const entry = _makeMultiDest();
            const ke = entry.toKafkaEntry({
                site: 'siteA',
                destination: 'arn:aws:s3:::bucket-b',
                role: 'arn:aws:iam::333:role/dst',
            });
            const payload = JSON.parse(ke.message);

            assert.strictEqual(payload.site, 'siteA');
            assert.strictEqual(payload.destination, 'arn:aws:s3:::bucket-b');
            assert.strictEqual(payload.role, 'arn:aws:iam::333:role/dst');
        });

        it('createFromKafkaEntry round-trips destination and role', () => {
            const entry = _makeMultiDest();
            const ke = entry.toKafkaEntry({
                site: 'siteA',
                destination: 'arn:aws:s3:::bucket-b',
                role: 'arn:aws:iam::333:role/dst',
            });
            const restored = QueueEntry.createFromKafkaEntry({ value: ke.message });

            assert.strictEqual(restored.getSite(), 'siteA');
            assert.strictEqual(restored.getDestination(), 'arn:aws:s3:::bucket-b');
            assert.strictEqual(restored.getRole(), 'arn:aws:iam::333:role/dst');
        });
    });

    describe('toRetryEntry', () => {
        it('should not clear dataStoreVersionId when retrying', () => {
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            const dataStoreVersionId = entry.getReplicationSiteDataStoreVersionId({ site: 'sf' });
            const retryEntry = entry.toRetryEntry({ site: 'sf' });

            assert.strictEqual(retryEntry.getReplicationSiteDataStoreVersionId({ site: 'sf' }), dataStoreVersionId);
        });

        it('should only include failed site details', () => {
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            const retryEntry = entry.toRetryEntry({ site: 'sf' });

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
