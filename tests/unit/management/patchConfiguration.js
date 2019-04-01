const assert = require('assert');
const async = require('async');
const Metadata = require('arsenal').storage.metadata.MetadataWrapper;
const BucketInfo = require('arsenal').models.BucketInfo;

const patchConfiguration =
    require('../../../lib/management/patchConfiguration');
const Config = require('../../../conf/Config');
const testConfig = require('../../config.json');
const fakeLogger = require('../../utils/fakeLogger');

const PATCH_VERSION = 2;
const locationTypeMatch = {
    'location-mem-v1': 'mem',
    'location-file-v1': 'file',
    'location-azure-v1': 'azure',
    'location-do-spaces-v1': 'aws_s3',
    'location-aws-s3-v1': 'aws_s3',
    'location-wasabi-v1': 'aws_s3',
    'location-gcp-v1': 'gcp',
    'location-scality-ring-s3-v1': 'aws_s3',
    'location-ceph-radosgw-s3-v1': 'aws_s3',
};
const mongoConfig = {
    replicaSetHosts: testConfig.queuePopulator.mongo.replicaSetHosts,
    database: 'metadata',
    writeConcern: 'majority',
    replicaSet: 'rs0',
    readPreference: 'primary',
    logger: fakeLogger,
};
const configOverlay = {
    version: PATCH_VERSION + 1,
    locations: {
        'location-1': {
            details: {
                accessKey: 'myaccesskey',
            },
            locationType: 'location-scality-ring-s3-v1',
        },
        'location-2': {
            details: {
                accessKey: 'anotheraccesskey',
            },
            locationType: 'location-file-v1',
        },
        'location-3': {
            details: {
                accessKey: 'anotheraccesskey',
            },
            locationType: 'location-azure-v1',
        },
    },
};

function createBucketMDObject(bucketName, locationName, ingestion) {
    const mockCreationDate = new Date().toString();
    return new BucketInfo(bucketName, 'owner', 'ownerDisplayName',
        mockCreationDate, null, null, null, null, null, null, locationName,
        null, null, null, null, null, null, null, ingestion);
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
        // empty configs
        Config.setBootstrapList({});
        Config.setIngestionBuckets({}, []);
    });

    after(() => {
        const client = this.md.client;
        client.db.dropDatabase();
    });

    it('should not patch any configurations when a version has not been set',
    done => {
        const conf = {
            version: undefined,
        };
        // regardless of overlay version, there should be no update to configs
        const overlayVersion = PATCH_VERSION;
        patchConfiguration(overlayVersion, conf, this.md, true, fakeLogger,
        (err, version) => {
            assert.ifError(err);

            const bootstrapList = Config.getBootstrapList();
            const ingestionBuckets = Config.getIngestionBuckets();
            assert(version === undefined);
            assert.strictEqual(bootstrapList.length, 0);
            assert.strictEqual(ingestionBuckets.length, 0);
            done();
        });
    });

    describe('bootstrap list', () => {
        const defaultLocations = configOverlay.locations;
        const expectedLocations = Object.keys(defaultLocations)
            .reduce((list, b) => {
                if (!list.includes(b) &&
                    defaultLocations[b].locationType !== 'location-file-v1') {
                    list.push(b);
                }
                return list;
            }, []);

        it('should filter Scality locations from bootstrapList (Orbit ' +
        'specific)', done => {
            const overlayVersion = PATCH_VERSION;
            patchConfiguration(overlayVersion, configOverlay, this.md, true,
            fakeLogger, err => {
                assert.ifError(err);

                const bootstrapList = Config.getBootstrapList();
                const scalityLocationName = Object.keys(configOverlay.locations)
                    .find(l => (configOverlay.locations[l].locationType ===
                        'location-file-v1'));
                assert(!bootstrapList.find(l =>
                    l.site === scalityLocationName));
                done();
            });
        });

        it('should not patch bootstrap list with an old overlay version',
        done => {
            const prevOverlayVersion = PATCH_VERSION + 1;
            patchConfiguration(prevOverlayVersion, configOverlay, this.md, true,
            fakeLogger, (err, version) => {
                assert.ifError(err);

                const bootstrapList = Config.getBootstrapList();
                assert(version === undefined);
                assert.strictEqual(bootstrapList.length, 0);
                done();
            });
        });

        it('should patch bootstrap list with a new overlay version', done => {
            const overlayVersion = PATCH_VERSION;
            patchConfiguration(overlayVersion, configOverlay, this.md, true,
            fakeLogger, (err, version) => {
                assert.ifError(err);

                const bootstrapList = Config.getBootstrapList();
                assert.strictEqual(version, configOverlay.version);
                assert.strictEqual(
                    bootstrapList.length, expectedLocations.length);
                expectedLocations.forEach(location => {
                    assert(bootstrapList.find(l => l.site === location));
                });
                done();
            });
        });

        it('should correctly form bootstrap list config', done => {
            const overlayVersion = PATCH_VERSION;
            patchConfiguration(overlayVersion, configOverlay, this.md, true,
            fakeLogger, err => {
                assert.ifError(err);

                const bootstrapList = Config.getBootstrapList();
                bootstrapList.forEach(location => {
                    assert(location.site);
                    assert(location.type);
                    const locType =
                        configOverlay.locations[location.site].locationType;
                    assert.strictEqual(
                        location.type, locationTypeMatch[locType]);
                });
                done();
            });
        });
    });

    describe('ingestion bucket list', () => {
        const ingestionTypeMatch = {
            'location-scality-ring-s3-v1': 'scality_s3',
        };
        const expectedIngestionBuckets = [bucket1];

        it('should patch ingestion buckets config with an old overlay version',
        done => {
            let ingestionBuckets = Config.getIngestionBuckets();
            assert.strictEqual(ingestionBuckets.length, 0);

            const prevOverlayVersion = PATCH_VERSION + 1;
            patchConfiguration(prevOverlayVersion, configOverlay, this.md, true,
            fakeLogger, (err, version) => {
                assert.ifError(err);

                ingestionBuckets = Config.getIngestionBuckets();
                assert(version === undefined);
                assert.strictEqual(ingestionBuckets.length, 1);
                expectedIngestionBuckets.forEach(bucket => {
                    assert(ingestionBuckets.find(
                        b => b.zenkoBucket === bucket.getName()));
                });
                done();
            });
        });

        it('should patch ingestion buckets config with a new overlay version',
        done => {
            const overlayVersion = PATCH_VERSION;
            patchConfiguration(overlayVersion, configOverlay, this.md, true,
            fakeLogger, (err, version) => {
                assert.ifError(err);

                const ingestionBuckets = Config.getIngestionBuckets();
                assert.strictEqual(version, configOverlay.version);
                expectedIngestionBuckets.forEach(bucket => {
                    assert(ingestionBuckets.find(
                        b => b.zenkoBucket === bucket.getName()));
                });
                done();
            });
        });

        it('should not update ingestion buckets config if flag disabled',
        done => {
            const overlayVersion = PATCH_VERSION;
            patchConfiguration(overlayVersion, configOverlay, this.md,
            undefined, fakeLogger, (err, version) => {
                assert.ifError(err);

                const ingestionBuckets = Config.getIngestionBuckets();
                assert.strictEqual(ingestionBuckets.length, 0);

                // should update bootstrapList given overlay version has been
                // updated
                const bootstrapList = Config.getBootstrapList();
                assert.strictEqual(version, configOverlay.version);
                assert(bootstrapList.length > 0);

                done();
            });
        });

        it('should correctly form ingestion bucket list config', done => {
            const overlayVersion = PATCH_VERSION;
            patchConfiguration(overlayVersion, configOverlay, this.md, true,
            fakeLogger, err => {
                assert.ifError(err);

                const ingestionBuckets = Config.getIngestionBuckets();

                ingestionBuckets.forEach(bucket => {
                    assert(bucket.accessKey);
                    assert(bucket.zenkoBucket);
                    assert(bucket.locationConstraint);
                    assert(bucket.locationType);
                    const locs = configOverlay.locations;
                    const locType =
                        locs[bucket.locationConstraint].locationType;
                    assert.strictEqual(
                        bucket.locationType, ingestionTypeMatch[locType]);
                    assert.deepStrictEqual(
                        bucket.ingestion, { status: 'enabled' });
                });
                done();
            });
        });
    });
});
