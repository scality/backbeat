const assert = require('assert');
const async = require('async');
const Metadata = require('arsenal').storage.metadata.MetadataWrapper;
const BucketInfo = require('arsenal').models.BucketInfo;

const { patchConfiguration } =
    require('../../../lib/management/patchConfiguration');
const Config = require('../../../lib/Config');
const testConfig = require('../../config.json');
const fakeLogger = require('../../utils/fakeLogger');

const managementDatabaseName = 'PENSIEVE';
const tokenConfigurationKey = 'auth/zenko/remote-management-token';
const privateKey = '-----BEGIN RSA PRIVATE KEY-----\r\nMIIEowIBAAKCAQEAj13sSYE40lAX2qpBvfdGfcSVNtBf8i5FH+E8FAhORwwPu+2S\r\n3yBQbgwHq30WWxunGb1NmZL1wkVZ+vf12DtxqFRnMA08LfO4oO6oC4V8XfKeuHyJ\r\n1qlaKRINz6r9yDkTHtwWoBnlAINurlcNKgGD5p7D+G26Chbr/Oo0ZwHula9DxXy6\r\neH8/bJ5/BynyNyyWRPoAO+UkUdY5utkFCUq2dbBIhovMgjjikf5p2oWqnRKXc+JK\r\nBegr6lSHkkhyqNhTmd8+wA+8Cace4sy1ajY1t5V4wfRZea5vwl/HlyyKodvHdxng\r\nJgg6H61JMYPkplY6Gr9OryBKEAgq02zYoYTDfwIDAQABAoIBAAuDYGlavkRteCzw\r\nRU1LIVcSRWVcgIgDXTu9K8T0Ec0008Kkxomyn6LmxmroJbZ1VwsDH8s4eRH73ckA\r\nxrZxt6Pr+0lplq6eBvKtl8MtGhq1VDe+kJczjHEF6SQHOFAu/TEaPZrn2XMcGvRX\r\nO1BnRL9tepFlxm3u/06VRFYNWqqchM+tFyzLu2AuiuKd5+slSX7KZvVgdkY1ErKH\r\ngB75lPyhPb77C/6ptqUisVMSO4JhLhsD0+ekDVY982Sb7KkI+szdWSbtMx9Ek2Wo\r\ntXwJz7I8T7IbODy9aW9G+ydyhMDFmaEYIaDVFKJj5+fluNza3oQ5PtFNVE50GQJA\r\nsisGqfECgYEAwpkwt0KpSamSEH6qknNYPOwxgEuXWoFVzibko7is2tFPvY+YJowb\r\n68MqHIYhf7gHLq2dc5Jg1TTbGqLECjVxp4xLU4c95KBy1J9CPAcuH4xQLDXmeLzP\r\nJ2YgznRocbzAMCDAwafCr3uY9FM7oGDHAi5bE5W11xWx+9MlFExL3JkCgYEAvJp5\r\nf+JGN1W037bQe2QLYUWGszewZsvplnNOeytGQa57w4YdF42lPhMz6Kc/zdzKZpN9\r\njrshiIDhAD5NCno6dwqafBAW9WZl0sn7EnlLhD4Lwm8E9bRHnC9H82yFuqmNrzww\r\nzxBCQogJISwHiVz4EkU48B283ecBn0wT/fAa19cCgYEApKWsnEHgrhy1IxOpCoRh\r\nUhqdv2k1xDPN/8DUjtnAFtwmVcLa/zJopU/Zn4y1ZzSzjwECSTi+iWZRQ/YXXHPf\r\nl92SFjhFW92Niuy8w8FnevXjF6T7PYiy1SkJ9OR1QlZrXc04iiGBDazLu115A7ce\r\nanACS03OLw+CKgl6Q/RR83ECgYBCUngDVoimkMcIHHt3yJiP3ikeAKlRnMdJlsa0\r\nXWVZV4hCG3lDfRXsnEgWuimftNKf+6GdfYSvQdLdiQsCcjT5A4uLsQTByv5nf4uA\r\n1ZKOsFrmRrARzxGXhLDikvj7yP//7USkq+0BBGFhfuAvl7fMhPceyPZPehqB7/jf\r\nxX1LBQKBgAn5GgSXzzS0e06ZlP/VrKxreOHa5Z8wOmqqYQ0QTeczAbNNmuITdwwB\r\nNkbRqpVXRIfuj0BQBegAiix8om1W4it0cwz54IXBwQULxJR1StWxj3jo4QtpMQ+z\r\npVPdB1Ilb9zPV1YvDwRfdS1xsobzznAx56ecsXduZjs9mF61db8Q\r\n-----END RSA PRIVATE KEY-----\r\n'; // eslint-disable-line
const encryptedSecretKey = 'K5FyqZo5uFKfw9QBtn95o6vuPuD0zH/1seIrqPKqGnz8AxALNS' +
    'x6EeRq7G1I6JJpS1XN13EhnwGn2ipsml3Uf2fQ00YgEmImG8wzGVZm8fWotpVO4ilN4JGyQCah' +
    '81rNX4wZ9xHqDD7qYR5MyIERxR/osoXfctOwY7GGUjRKJfLOguNUlpaovejg6mZfTvYAiDF+PTO1' +
    'sKUYqHt1IfKQtsK3dov1EFMBB5pWM7sVfncq/CthKN5M+VHx9Y87qdoP3+7AW+RCBbSDOfQgxvqtS7' +
    'PIAf10mDl8k2kEURLz+RqChu4O4S0UzbEmtja7wa7WYhYKv/tM/QeW7kyNJMmnPg==';

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

function createConfig() {
    return {
        version: PATCH_VERSION + 1,
        locations: {
            'location-1': {
                details: {
                    accessKey: 'myaccesskey',
                    secretKey: encryptedSecretKey,
                },
                locationType: 'location-scality-ring-s3-v1',
            },
            'location-2': {
                details: {
                    accessKey: 'anotheraccesskey',
                    secretKey: encryptedSecretKey,
                },
                locationType: 'location-file-v1',
            },
            'location-3': {
                details: {
                    accessKey: 'anotheraccesskey',
                    secretKey: encryptedSecretKey,
                },
                locationType: 'location-azure-v1',
            },
        },
        instanceId: 'hello-zenko',
    };
}

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
            next => this.md.putObjectMD(
                managementDatabaseName,
                tokenConfigurationKey,
                { privateKey },
                {},
                fakeLogger,
                err => next(err),
            ),
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
        let configOverlay;
        let expectedLocations;
        beforeEach(() => {
            configOverlay = createConfig();
            expectedLocations = [
                'location-1',
                'location-3',
            ];
        });

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

        it('should not patch bootstrap list with an old overlay version', done => {
            const prevOverlayVersion = PATCH_VERSION + 1;
            patchConfiguration(
                prevOverlayVersion,
                configOverlay,
                this.md,
                true,
                fakeLogger,
                (err, version) => {
                    assert.ifError(err);

                    const bootstrapList = Config.getBootstrapList();
                    assert.strictEqual(version, undefined);
                    assert.strictEqual(bootstrapList.length, 0);
                    done();
                }
            );
        });

        it('should patch bootstrap list with a new overlay version', done => {
            const overlayVersion = PATCH_VERSION;
            patchConfiguration(overlayVersion, configOverlay, this.md, true,
            fakeLogger, err => {
                assert.ifError(err);

                const bootstrapList = Config.getBootstrapList();
                assert.strictEqual(bootstrapList.length, expectedLocations.length);
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

        let configOverlay;
        beforeEach(() => {
            configOverlay = createConfig();
        });

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
            fakeLogger, err => {
                assert.ifError(err);
                const ingestionBuckets = Config.getIngestionBuckets();
                expectedIngestionBuckets.forEach(bucket => {
                    assert(
                        ingestionBuckets.find(b => b.zenkoBucket === bucket.getName())
                    );
                });
                done();
            });
        });

        it('should not update ingestion buckets config if flag disabled', done => {
            const overlayVersion = PATCH_VERSION;
            patchConfiguration(
                overlayVersion,
                configOverlay,
                this.md,
                undefined,
                fakeLogger,
                err => {
                    assert.ifError(err);

                    const ingestionBuckets = Config.getIngestionBuckets();
                    assert.strictEqual(ingestionBuckets.length, 0);

                    // should update bootstrapList given overlay version has been
                    // updated
                    const bootstrapList = Config.getBootstrapList();
                    assert(bootstrapList.length > 0);

                    done();
            });
        });

        it('should correctly form ingestion bucket list config', done => {
            const overlayVersion = PATCH_VERSION;
            const conf = Object.assign({}, configOverlay);
            patchConfiguration(overlayVersion, conf, this.md, true,
            fakeLogger, err => {
                assert.ifError(err);

                const ingestionBuckets = Config.getIngestionBuckets();

                ingestionBuckets.forEach(bucket => {
                    assert(bucket.credentials.accessKey);
                    assert(bucket.credentials.secretKey);
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
