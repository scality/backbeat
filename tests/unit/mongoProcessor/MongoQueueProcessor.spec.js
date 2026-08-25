const assert = require('assert');
const sinon = require('sinon');

const MongoQueueProcessor =
    require('../../../extensions/mongoProcessor/MongoQueueProcessor');
const ObjectQueueEntry =
    require('../../../lib/models/ObjectQueueEntry');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const Config = require('../../../lib/Config');

function _makeProcessor(bootstrapList) {
    const proc = Object.create(MongoQueueProcessor.prototype);
    proc._bootstrapList = bootstrapList;
    return proc;
}

function _makeEntry(objectKey = 'docs/2024/report.pdf') {
    return new ObjectQueueEntry('bucket', `${objectKey} v`, {
        'md-model-version': 2,
        'replicationInfo': {
            status: '',
            backends: [],
            content: [],
            destination: '',
            storageClass: '',
            role: '',
            storageType: '',
        },
    });
}

function _makeBucketInfo(repCfg) {
    return {
        getReplicationConfiguration: () => repCfg,
        isNFS: () => false,
    };
}

describe('MongoQueueProcessor._updateReplicationInfo', () => {
    it('builds per-backend destination/role from per-rule fields', () => {
        const proc = _makeProcessor([
            { site: 'crr-a', type: 'scality' },
            { site: 'crr-b', type: 'scality' },
        ]);
        const entry = _makeEntry();
        const bucketInfo = _makeBucketInfo({
            role: 'arn:aws:iam::111:role/src,arn:aws:iam::000:role/repRule',
            destination: 'arn:aws:s3:::legacy-bucket',
            rules: [{
                id: 'r-a', prefix: '', enabled: true, priority: 1,
                storageClass: 'crr-a',
                destination: 'arn:aws:s3:::bucket-a',
                account: '222',
            }, {
                id: 'r-b', prefix: '', enabled: true, priority: 2,
                storageClass: 'crr-b',
                destination: 'arn:aws:s3:::bucket-b',
                account: '333',
            }],
        });

        proc._updateReplicationInfo(entry, bucketInfo, ['DATA', 'METADATA']);

        const info = entry.getReplicationInfo();
        assert.strictEqual(info.status, 'PENDING');
        assert.strictEqual(info.role, 'arn:aws:iam::111:role/src');
        assert.strictEqual(info.destination, undefined);
        const backendsBySite = Object.fromEntries(
            info.backends.map(b => [b.site, b]));
        assert.deepStrictEqual(
            info.backends.map(b => b.site).sort(), ['crr-a', 'crr-b']);
        assert.strictEqual(backendsBySite['crr-a'].destination,
            'arn:aws:s3:::bucket-a');
        assert.strictEqual(backendsBySite['crr-a'].role,
            'arn:aws:iam::222:role/repRule');
        assert.strictEqual(backendsBySite['crr-b'].destination,
            'arn:aws:s3:::bucket-b');
        assert.strictEqual(backendsBySite['crr-b'].role,
            'arn:aws:iam::333:role/repRule');
    });

    it('derives per-rule role via account substitution when absent', () => {
        const proc = _makeProcessor([{ site: 'crr-a', type: 'scality' }]);
        const entry = _makeEntry();
        const bucketInfo = _makeBucketInfo({
            role: 'arn:aws:iam::111:role/src,arn:aws:iam::000:role/repRule',
            rules: [{
                id: 'r-a', prefix: '', enabled: true,
                storageClass: 'crr-a',
                destination: 'arn:aws:s3:::bucket-a',
                account: '222',
            }],
        });

        proc._updateReplicationInfo(entry, bucketInfo, ['DATA']);

        const [backend] = entry.getReplicationInfo().backends;
        assert.strictEqual(backend.role, 'arn:aws:iam::222:role/repRule');
    });

    it('dedups CRR backends on (site, destination, role)', () => {
        const proc = _makeProcessor([{ site: 'crr-a', type: 'scality' }]);
        const entry = _makeEntry();
        const bucketInfo = _makeBucketInfo({
            role: 'arn:aws:iam::111:role/src,arn:aws:iam::000:role/x',
            rules: [{
                id: 'low', prefix: '', enabled: true, priority: 1,
                storageClass: 'crr-a',
                destination: 'arn:aws:s3:::bucket-a',
                account: '222',
            }, {
                id: 'high', prefix: 'docs', enabled: true, priority: 10,
                storageClass: 'crr-a',
                destination: 'arn:aws:s3:::bucket-a',
                account: '222',
            }],
        });

        proc._updateReplicationInfo(entry, bucketInfo, ['DATA']);

        const backends = entry.getReplicationInfo().backends;
        assert.strictEqual(backends.length, 1);
        assert.strictEqual(backends[0].role, 'arn:aws:iam::222:role/x');
    });

    it('omits destination/role on cloud backends', () => {
        const proc = _makeProcessor([{ site: 'cloud-a', type: 'aws_s3' }]);
        const entry = _makeEntry();
        const bucketInfo = _makeBucketInfo({
            role: 'arn:aws:iam::111:role/src',
            rules: [{
                id: 'r-a', prefix: '', enabled: true,
                storageClass: 'cloud-a',
                destination: 'arn:aws:s3:::ignored',
                account: '222',
            }],
        });

        proc._updateReplicationInfo(entry, bucketInfo, ['DATA']);

        const [backend] = entry.getReplicationInfo().backends;
        assert.strictEqual(backend.site, 'cloud-a');
        assert.strictEqual(backend.destination, undefined);
        assert.strictEqual(backend.role, undefined);
    });

    it('handles legacy V1 comma-separated storageClass form', () => {
        const proc = _makeProcessor([
            { site: 'crr-a', type: 'scality' },
            { site: 'crr-b', type: 'scality' },
        ]);
        const entry = _makeEntry();
        const bucketInfo = _makeBucketInfo({
            role: 'arn:aws:iam::111:role/src,arn:aws:iam::222:role/dst',
            destination: 'arn:aws:s3:::legacy-bucket',
            rules: [{
                id: 'r1', prefix: '', enabled: true,
                storageClass: 'crr-a,crr-b',
            }],
        });

        proc._updateReplicationInfo(entry, bucketInfo, ['DATA']);

        const sites = entry.getReplicationInfo().backends.map(b => b.site);
        assert.deepStrictEqual(sites.sort(), ['crr-a', 'crr-b']);
        // Each CRR backend falls back to top-level destination/role.
        entry.getReplicationInfo().backends.forEach(b => {
            assert.strictEqual(b.destination, 'arn:aws:s3:::legacy-bucket');
            assert.strictEqual(b.role, 'arn:aws:iam::222:role/dst');
        });
    });

    it('clears replicationInfo when the bucket has no replication configuration', () => {
        const proc = _makeProcessor([{ site: 'crr-a', type: 'scality' }]);
        const entry = _makeEntry();
        proc._updateReplicationInfo(entry, _makeBucketInfo(null), ['DATA']);
        // No replication config → backends stay empty (the reset that
        // runs before the early return still wipes any stale state).
        assert.deepStrictEqual(entry.getReplicationInfo().backends, []);
    });

    it('produces no backends when no rule matches', () => {
        const proc = _makeProcessor([{ site: 'crr-a', type: 'scality' }]);
        const entry = _makeEntry('other/path');
        const bucketInfo = _makeBucketInfo({
            role: 'arn:aws:iam::111:role/src',
            rules: [{
                id: 'r-a', prefix: 'docs', enabled: true,
                storageClass: 'crr-a',
                destination: 'arn:aws:s3:::bucket-a',
                account: '222',
            }],
        });

        proc._updateReplicationInfo(entry, bucketInfo, ['DATA']);

        assert.deepStrictEqual(entry.getReplicationInfo().backends, []);
    });

    it('produces mixed CRR + cloud backends in one call', () => {
        const proc = _makeProcessor([
            { site: 'crr-a', type: 'scality' },
            { site: 'cloud-b', type: 'aws_s3' },
        ]);
        const entry = _makeEntry();
        const bucketInfo = _makeBucketInfo({
            role: 'arn:aws:iam::111:role/src,arn:aws:iam::000:role/repRule',
            rules: [{
                id: 'r-crr',
                prefix: '',
                enabled: true,
                storageClass: 'crr-a',
                destination: 'arn:aws:s3:::bucket-a',
                account: '222',
            },
            {
                id: 'r-cloud',
                prefix: '',
                enabled: true,
                storageClass: 'cloud-b',
            }],
        });

        proc._updateReplicationInfo(entry, bucketInfo, ['DATA', 'METADATA']);

        const info = entry.getReplicationInfo();
        assert.strictEqual(info.backends.length, 2);
        const bySite = Object.fromEntries(info.backends.map(b => [b.site, b]));

        // CRR backend carries destination + role per-rule.
        assert.strictEqual(bySite['crr-a'].destination, 'arn:aws:s3:::bucket-a');
        assert.strictEqual(bySite['crr-a'].role, 'arn:aws:iam::222:role/repRule');

        // Cloud backend has bare {site, status, dataStoreVersionId} only.
        assert.strictEqual(bySite['cloud-b'].destination, undefined);
        assert.strictEqual(bySite['cloud-b'].role, undefined);
        assert.strictEqual(bySite['cloud-b'].status, 'PENDING');
    });
});

describe('MongoQueueProcessor.start', () => {
    afterEach(() => {
        sinon.restore();
    });

    function startProcessor() {
        sinon.stub(BackbeatConsumer.prototype, '_init');
        sinon.stub(Config, 'getBootstrapList').returns([]);
        sinon.stub(Config, 'on');

        const proc = _makeProcessor([]);
        proc.logger = { info: () => {}, error: () => {}, fatal: () => {} };
        proc.kafkaConfig = { hosts: 'localhost:9092' };
        proc.mongoProcessorConfig = {
            topic: 'backbeat-ingestion',
            groupId: 'backbeat-ingestion-group',
            concurrency: 5,
        };
        proc._setupMetricsClients = cb => cb();
        proc._mongoClient = { setup: cb => cb() };

        proc.start();

        return proc;
    }

    it('starts the consumer at the earliest offset', done => {
        const proc = startProcessor();

        setImmediate(() => {
            assert.strictEqual(proc._consumer._fromOffset, 'earliest');
            done();
        });
    });
});
