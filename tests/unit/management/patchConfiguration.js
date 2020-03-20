const assert = require('assert');
const async = require('async');
const Metadata = require('arsenal').storage.metadata.MetadataWrapper;
const BucketInfo = require('arsenal').models.BucketInfo;

const patchConfiguration =
    require('../../../lib/management/patchConfiguration');
const Config = require('../../../lib/Config');
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
const credentials = {
    instanceId: 'b226a330-dd52-4344-b06c-db8fcf32d210',
    issueDate: '2020-02-10T20:55:12Z',
    publicKey: '-----BEGIN PUBLIC KEY-----\r\n'
        + 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAgTyWzMwhBz5IKRhiKRfK\r\n'
        + '3xPOl8lJiHo7HPLkX7/fVsFZtAGF1ii4mWVadt0I+5VMxFeISIAdNsOuRp2Yw+tj\r\n'
        + 'sIiCm7bgZM3Ta6QHqme7omhthV8h13/TPrNRxTcJz3XsAYkzF1m7cn6DXsueOfgH\r\n'
        + 'qHNcd14dE8aeHgg7Ei3ZaGX1ty6MgmYSK57qWadD9AaAB1py8LMaN6sHp+F/HSRX\r\n'
        + 'Os8ZvzF5F1NkIF5FDNec8AAicChfc0+ShaTPLLj8Y4ZA3O9YzbbO6LG9i0t/AnbJ\r\n'
        + 'ZqfFH5LthAaHT9NktSy8U1IEmOz2L54mwFQzuu4fqYdQRBc7svb4XWFozyZXX0uR\r\n'
        + 'NQIDAQAB\r\n'
        + '-----END PUBLIC KEY-----\r\n',
    token: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOiIyMDIwLTAyLTE3VDIwOj'
        + 'U1OjEyWiIsImlhdCI6IjIwMjAtMDItMTBUMjA6NTU6MTJaIiwiaW5zdGFuY2VJZCI6Im'
        + 'IyMjZhMzMwLWRkNTItNDM0NC1iMDZjLWRiOGZjZjMyZDIxMCIsInJvbGUiOiJpbnN0YW'
        + '5jZSJ9.4snlczKrBJNUH20NHWxJfHBxP6YC9fTlCUhne2tWMSw',
    privateKey: '-----BEGIN RSA PRIVATE KEY-----\r\n'
        + 'MIIEowIBAAKCAQEAgTyWzMwhBz5IKRhiKRfK3xPOl8lJiHo7HPLkX7/fVsFZtAGF\r\n'
        + '1ii4mWVadt0I+5VMxFeISIAdNsOuRp2Yw+tjsIiCm7bgZM3Ta6QHqme7omhthV8h\r\n'
        + '13/TPrNRxTcJz3XsAYkzF1m7cn6DXsueOfgHqHNcd14dE8aeHgg7Ei3ZaGX1ty6M\r\n'
        + 'gmYSK57qWadD9AaAB1py8LMaN6sHp+F/HSRXOs8ZvzF5F1NkIF5FDNec8AAicChf\r\n'
        + 'c0+ShaTPLLj8Y4ZA3O9YzbbO6LG9i0t/AnbJZqfFH5LthAaHT9NktSy8U1IEmOz2\r\n'
        + 'L54mwFQzuu4fqYdQRBc7svb4XWFozyZXX0uRNQIDAQABAoIBAFVq6nDp6lqTO7aN\r\n'
        + 'uzNV2mye9skz7TobL77ueysd8kuw980VxJPLzlb0ulodtbYy885B3H2uz6BGrYVW\r\n'
        + '3IWBqx4e29R3htCZicd3XumuLkIlq12fhwqcHc8vTjh/LCjG0/of6HjighYmsEWT\r\n'
        + 'Zz0BRm578P2kYquTdyZ6YjCdxThOWulHvONoyRbUYzm8Y8WrF0VJOFJBoXQIM0bA\r\n'
        + 'HQ4qLaBc+CMD3KD3YE+fs0Yn4AEg1V/dNnBwa5oZ+rm/Fo0MIFlamK460tNh72fS\r\n'
        + '9gq067nbPoj4J2FhX9JcpVcP4jgGenG4PAweFqpTjf7MDIerNaobrkHqCU1YhKoD\r\n'
        + 'B85+SoECgYEAvpK/dP+Roport0vFLDMlaINA121tlQDEB0hjmL0AGPbX9Gtal8ew\r\n'
        + 't6P14xgmhwC6Cad1qe7M31SrLe4JdCnRdsqhL0119kDoqDIg7F32JeqPPZ2TJubM\r\n'
        + 'bDlbFbVr+PNi/5JsTsIBwAFxw+FfFQ6hd54UcGmopjD4Pkpnz0xifJ0CgYEArZsL\r\n'
        + 'gaMWvmMPJFBWjyvz201Yq4tAG8TsTQtiK8sePWZAhalYmJzMD20cyaePB+8IT2OG\r\n'
        + 'uYDIOcKc+ptGEkKIJfnYCyRd3snw7KEjBUOhiAuxUFSL9daY624+TBfPvlcEZkGo\r\n'
        + 'gEEVck2ifPrbEoa1629GnaCtsLbibbZ2Imlx53kCgYB/Nq6fh0rMZGXyQZ4pVysN\r\n'
        + 'jTBnniCcappw0h3KA8Bg6cZW3qLm1uJcdBLbuW9eh1mowCSHf2U7X+W1D0U4SgIN\r\n'
        + 'bk4SqX6pF8M3I99eaYq63M0psFpeiYrEY7Ut1KFy2eWn/TJXkKJibZRn0bYK9G1M\r\n'
        + '0DWMpLtz1RShYEHT24WS8QKBgCCyPcWJqMPstjJZqyPF3GSmOZf9XvKb8QFFrpSe\r\n'
        + '6bAYrPg/f78mcMxK+YwFMcwFuePx07LmTU+LlrMgQV85BplYZ9cZX1CRaf23D3hz\r\n'
        + 'V13fDeMaOU3wv9Y/ah48sdSgYOS9YvXczCQ9+Ode+5mOo36W847Gb1AD8btGDRPI\r\n'
        + 'FjrJAoGBAKpPju9/a6Ct4CCFsCXkCgw3m2FuF2pFr/HPfWFWUvT3C2eVDPOY6iWH\r\n'
        + 'Qa/T9Z3btWbP21ulsJdROTtsjtLX7uabc5JyvpAkSIRjJbEBsw20HYdgPRtYQC8p\r\n'
        + 'uyfSvngWG7+lbga5EU5/jYdSlolO/ZslBoqOGv00CSaGSP6d7Ca0\r\n'
        + '-----END RSA PRIVATE KEY-----\r\n',
};

const configOverlay = {
    version: PATCH_VERSION + 1,
    locations: {
        'location-1': {
            name: 'location-1',
            details: {
                accessKey: 'myaccesskey',
                secretKey: 'eRXuAQe91s7vrGV97yc58FyiGPfme51FSjFwbj/DqOmKCWkD8W6'
                    + 'IoLat2FF3ouNP+cBt9uz4OmYq3dI6hGpNwUAI5GED6Tn2b5aV04AKMeB'
                    + 'DQZTX4VcrYsnoWrfHlSila3ugBRjCd6lkPe0hkQOKam/uRZ0C0W3gZPC'
                    + 'QcpklYBurgS+mdxRt2H8qu5PCTUTA685Bkpw4fgLab5Xsi9vpeHtct61'
                    + 'G2XhX/2TW0DK6wsxmU4lxmAxT/lSxTXdC50DwOkX4yZA2BmAfsBosGqK'
                    + 'RU4f68K18zqxMd9ck1DAyhOYzvPVNxTK4Kexasce5yWO+q8jyA1bJNPq'
                    + 'igCjeU5AyTg==',
            },
            locationType: 'location-scality-ring-s3-v1',
        },
        'location-2': {
            name: 'location-2',
            details: {
                accessKey: 'anotheraccesskey',
                secretKey: 'IPbwvQ829CPt+RuAQev9bRd+HmJWDDY1uG4ustacc3n5ecT4Ijr'
                    + 'BuGtHznX5Xwbc8DUE3JFPQ2JdaiRYte8qy06Q48sSKf4FCF7FudbcmYa'
                    + 'ZL0eXVjYM5R0vlVyVLtWtUmJtg41J+nOla94dqP12fe/4qnaQsHH+rt9'
                    + 'VMDo41mQ6PzG+OIwlsFVpR/WzdljcLIDWy2wlr+iSgB/w5AD4ivYNmoq'
                    + 'memkQk0iZbe4UUdKhRRyKr+7ra0pNVoVT50QWmxFcRDPikoaiYGGa3uI'
                    + 'lOt3Qo2vtmPGN6eVc3g/Bs2UcbHQANyCXfJRjEMIOY3AjnRx9bA+mrIR'
                    + 'mYmNXg/HJiA==',
            },
            locationType: 'location-file-v1',
        },
    },
    instanceId: 'hello-zenko',
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
        // empty configs
        Config.setLocationConstraints({});
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
        patchConfiguration(overlayVersion, conf, credentials, this.md, true, fakeLogger,
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
            patchConfiguration(overlayVersion, configOverlay, credentials, this.md, true,
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
            patchConfiguration(prevOverlayVersion, configOverlay, credentials, this.md, true,
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
            patchConfiguration(overlayVersion, configOverlay, credentials, this.md, true,
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
            patchConfiguration(overlayVersion, configOverlay, credentials, this.md, true,
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
            patchConfiguration(prevOverlayVersion, configOverlay, credentials, this.md, true,
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
            patchConfiguration(overlayVersion, configOverlay, credentials, this.md, true,
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
            patchConfiguration(overlayVersion, configOverlay, credentials, this.md,
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
            patchConfiguration(overlayVersion, configOverlay, credentials, this.md, true,
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
