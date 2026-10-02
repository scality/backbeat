const assert = require('assert');

const { updateIngestionBuckets } =
    require('../../../lib/management/patchConfiguration');
const Config = require('../../../lib/Config');
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

// shape returned by MetadataWrapper.getIngestionBuckets()
function createBucket(name, locationConstraint, ingestion) {
    return { name, locationConstraint, ingestion };
}

describe('patchConfiguration', () => {
    describe('updateIngestionBuckets', () => {
        const bucket1 = createBucket('bucket-1', 'location-1',
            { status: 'enabled' });
        // no ingestion set on bucket2
        const bucket2 = createBucket('bucket-2', 'location-2', null);
        // ingestion enabled but not a backbeat ingestion location
        const bucket3 = createBucket('bucket-3', 'location-2',
            { status: 'enabled' });

        let metadata;

        beforeEach(() => {
            Config.setIngestionBuckets({}, []);
            metadata = {
                getIngestionBuckets: (log, cb) =>
                    process.nextTick(cb, null, [bucket1, bucket2, bucket3]),
            };
        });

        it('should forward metadata errors', done => {
            metadata.getIngestionBuckets = (log, cb) =>
                process.nextTick(cb, new Error('boom'));
            updateIngestionBuckets(locations, metadata, fakeLogger, err => {
                assert(err);
                assert.strictEqual(err.message, 'boom');
                assert.strictEqual(Config.getIngestionBuckets().length, 0);
                done();
            });
        });

        it('should only keep buckets on a backbeat ingestion location', done => {
            updateIngestionBuckets(locations, metadata, fakeLogger, err => {
                assert.ifError(err);
                const ingestionBuckets = Config.getIngestionBuckets();
                assert.strictEqual(ingestionBuckets.length, 1);
                assert.strictEqual(ingestionBuckets[0].zenkoBucket, 'bucket-1');
                done();
            });
        });

        it('should correctly form ingestion bucket list config', done => {
            updateIngestionBuckets(locations, metadata, fakeLogger, err => {
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
