const assert = require('assert');
const async = require('async');
const Metadata = require('@scality/arsenal').storage.metadata.MetadataWrapper;
const BucketInfo = require('@scality/arsenal').models.BucketInfo;

const { updateIngestionBuckets } =
    require('../../../lib/management/patchConfiguration');
const Config = require('../../../lib/Config');
const testConfig = require('../../config.json');
const fakeLogger = require('../../utils/fakeLogger');

const locations = {
    'location-1': {
        details: {
            accessKey: 'myaccesskey',
            secretKey: 'mysecretkey',
        },
        locationType: 'location-scality-ring-s3-v1',
    },
    'location-2': {
        details: {
            accessKey: 'anotheraccesskey',
            secretKey: 'anothersecretkey',
        },
        locationType: 'location-file-v1',
    },
};
const mongoConfig = {
    replicaSetHosts: testConfig.queuePopulator.mongo.replicaSetHosts,
    database: 'metadata',
    writeConcern: 'majority',
    replicaSet: 'rs0',
    readPreference: 'primary',
    logger: fakeLogger,
};

function createBucketMDObject(bucketName, locationName, ingestion) {
    const mockCreationDate = new Date().toString();
    return new BucketInfo(bucketName, 'owner', 'ownerDisplayName',
        mockCreationDate, null, null, null, null, null, null, locationName,
        null, null, null, null, null, null, null, null, ingestion);
}

describe('patchConfiguration', () => {
    const bucket1 = createBucketMDObject('bucket-1', 'location-1',
        { status: 'enabled' });
    // no ingestion set on bucket2
    const bucket2 = createBucketMDObject('bucket-2', 'location-2', null);
    // ingestion enabled but not a backbeat ingestion location
    const bucket3 = createBucketMDObject('bucket-3', 'location-2',
        { status: 'enabled' });

    before(done => {
        async.waterfall([
            next => {
                this.md = new Metadata('mongodb', { mongodb: mongoConfig },
                    null, fakeLogger);
                this.md.setup(next);
            },
            // populate mongo with buckets
            next => this.md.createBucket('bucket-1', bucket1, fakeLogger, next),
            next => this.md.createBucket('bucket-2', bucket2, fakeLogger, next),
            next => this.md.createBucket('bucket-3', bucket3, fakeLogger, next),
        ], done);
    });

    beforeEach(() => {
        Config.setIngestionBuckets({}, []);
    });

    after(() => {
        const client = this.md.client;
        client.db.dropDatabase();
    });

    describe('ingestion bucket list', () => {
        it('should only keep buckets on a backbeat ingestion location', done => {
            updateIngestionBuckets(locations, this.md, fakeLogger, err => {
                assert.ifError(err);
                const ingestionBuckets = Config.getIngestionBuckets();
                assert.strictEqual(ingestionBuckets.length, 1);
                assert.strictEqual(ingestionBuckets[0].zenkoBucket,
                    bucket1.getName());
                done();
            });
        });

        it('should correctly form ingestion bucket list config', done => {
            updateIngestionBuckets(locations, this.md, fakeLogger, err => {
                assert.ifError(err);
                const [bucket] = Config.getIngestionBuckets();
                assert.deepStrictEqual(bucket, {
                    accessKey: 'myaccesskey',
                    secretKey: 'mysecretkey',
                    locationType: 'scality_s3',
                    zenkoBucket: 'bucket-1',
                    ingestion: { status: 'enabled' },
                    locationConstraint: 'location-1',
                });
                done();
            });
        });
    });
});
