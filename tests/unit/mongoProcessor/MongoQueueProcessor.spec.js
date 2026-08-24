const assert = require('assert');
const sinon = require('sinon');

const { VersionID, VersioningConstants } = require('arsenal').versioning;

const MongoQueueProcessor =
    require('../../../extensions/mongoProcessor/MongoQueueProcessor');
const ObjectQueueEntry =
    require('../../../lib/models/ObjectQueueEntry');
const DeleteOpQueueEntry =
    require('../../../lib/models/DeleteOpQueueEntry');

const VID_SEP = VersioningConstants.VersionId.Separator;

// see conf/locationConfig.json
const CRR_LOCATION = 'location-crr-source';
const LOCAL_LOCATION = 'us-east-1';

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

describe('MongoQueueProcessor._processDeleteOpQueueEntry', () => {
    const bucket = 'cleanroom-bucket';
    const objectKey = 'docs/report.pdf';
    const versionId = '98765432109876999999RG001  1';
    const encodedVersionId = VersionID.encode(versionId);

    function _makeLog() {
        const log = {
            debug: () => {},
            info: () => {},
            error: () => {},
            warn: () => {},
            getSerializedUids: () => 'req-uid',
        };
        log.end = () => log;
        return log;
    }

    function _makeDeleteProcessor(zenkoObjMd, gcProducer) {
        const proc = Object.create(MongoQueueProcessor.prototype);
        proc.logger = { debug: () => {} };
        proc._gcProducer = gcProducer;
        proc._mongoClient = {
            deleteObject: sinon.stub().callsFake((b, k, opts, log, cb) => cb()),
        };
        proc._getZenkoObjectMetadata =
            sinon.stub().callsFake((log, entry, vid, cb) => cb(null, zenkoObjMd));
        proc._produceMetricCompletionEntry = () => {};
        proc._normalizePendingMetric = () => {};
        return proc;
    }

    function _makeEntry() {
        return new DeleteOpQueueEntry(bucket, `${objectKey}${VID_SEP}${versionId}`, {});
    }

    it('deletes a localized version and publishes its local data for GC', done => {
        const zenkoObjMd = {
            'dataStoreName': LOCAL_LOCATION,
            'owner-id': 'owner-canonical-id',
            'content-md5': 'etag-value',
            'location': [{ dataStoreName: LOCAL_LOCATION, key: 'local-data-key' }],
        };
        const gcProducer = { publishActionEntry: sinon.stub() };
        const proc = _makeDeleteProcessor(zenkoObjMd, gcProducer);

        proc._processDeleteOpQueueEntry(_makeLog(), _makeEntry(), CRR_LOCATION, {}, err => {
            assert.ifError(err);
            assert.strictEqual(proc._mongoClient.deleteObject.callCount, 1);
            assert.strictEqual(gcProducer.publishActionEntry.callCount, 1);

            const gcEntry = gcProducer.publishActionEntry.firstCall.args[0];
            assert.strictEqual(gcEntry.getActionType(), 'deleteData');
            assert.deepStrictEqual(gcEntry.getAttribute('target.locations'),
                zenkoObjMd.location);
            assert.strictEqual(gcEntry.getAttribute('target.owner'), 'owner-canonical-id');
            assert.strictEqual(gcEntry.getAttribute('serviceName'), 'md-ingestion');
            // the version is gone: no conditional header must be sent by the GC
            assert.strictEqual(gcEntry.getAttribute('source').lastModified, undefined);
            assert.strictEqual(gcEntry.getContextAttribute('versionId'), encodedVersionId);
            done();
        });
    });

    it('does not publish anything for a version which was never localized', done => {
        const zenkoObjMd = {
            dataStoreName: CRR_LOCATION,
            location: [{
                dataStoreName: CRR_LOCATION,
                key: objectKey,
                dataStoreVersionId: encodedVersionId,
            }],
        };
        const gcProducer = { publishActionEntry: sinon.stub() };
        const proc = _makeDeleteProcessor(zenkoObjMd, gcProducer);

        proc._processDeleteOpQueueEntry(_makeLog(), _makeEntry(), CRR_LOCATION, {}, err => {
            assert.ifError(err);
            assert.strictEqual(proc._mongoClient.deleteObject.callCount, 1);
            assert.strictEqual(gcProducer.publishActionEntry.callCount, 0);
            done();
        });
    });

    it('still ignores an object transitioned outside of a clean room', done => {
        const zenkoObjMd = {
            dataStoreName: 'location-dmf-v1',
            location: [{ dataStoreName: 'location-dmf-v1', key: 'cold-key' }],
        };
        const gcProducer = { publishActionEntry: sinon.stub() };
        const proc = _makeDeleteProcessor(zenkoObjMd, gcProducer);

        proc._processDeleteOpQueueEntry(_makeLog(), _makeEntry(), LOCAL_LOCATION, {}, err => {
            assert.ifError(err);
            assert.strictEqual(proc._mongoClient.deleteObject.callCount, 0);
            assert.strictEqual(gcProducer.publishActionEntry.callCount, 0);
            done();
        });
    });

    it('deletes the metadata even when no garbage collector is configured', done => {
        const zenkoObjMd = {
            dataStoreName: LOCAL_LOCATION,
            location: [{ dataStoreName: LOCAL_LOCATION, key: 'local-data-key' }],
        };
        const proc = _makeDeleteProcessor(zenkoObjMd, null);

        proc._processDeleteOpQueueEntry(_makeLog(), _makeEntry(), CRR_LOCATION, {}, err => {
            assert.ifError(err);
            assert.strictEqual(proc._mongoClient.deleteObject.callCount, 1);
            done();
        });
    });
});
