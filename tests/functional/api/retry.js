const assert = require('assert');
const async = require('async');
const http = require('http');
const querystring = require('querystring');
const { RedisClient } = require('arsenal').metrics;
const { StatsModel } = require('arsenal').metrics;

const config = require('../../../conf/Config');
const getUrl = require('../utils/getUrl');
const { addMembers, addManyMembers } = require('../utils/sortedSetHelpers');
const { makeRetryPOSTRequest, getRequest, getResponseBody } =
    require('../utils/httpHelpers');
const fakeLogger = require('../../utils/fakeLogger');

const redisConfig = { host: '127.0.0.1', port: 6379 };
const redisClient = new RedisClient(redisConfig, fakeLogger);
const useRoleAuth = config.extensions.replication.source.auth.type === 'role';
const TEST_REDIS_KEY_FAILED_CRR = 'test:bb:crr:failed';
// Version ID calculated from the mock object MD.
const testVersionId =
    '39383530303038363133343437313939393939395247303031202030';
const testRole = 'arn:aws:iam::604563867484:role';

// Delete all Redis keys.
function deleteKeys(redisClient, cb) {
    redisClient.batch([['flushall']], cb);
}

// Get the member of the sorted set.
function getMember(baseMember) {
    const schemaRole = querystring.escape(testRole);
    return useRoleAuth ? `${baseMember}:${schemaRole}` : baseMember;
}

// If using IAM role for authentication, add the Role property.
function addRoleProp(data) {
    if (!useRoleAuth) {
        return data;
    }
    return data.map(responseObject =>
        Object.assign({}, responseObject, { Role: testRole }));
}

describe('CRR Retry routes', () => {
    const retryPaths = [
        '/_/crr/failed',
        '/_/crr/failed/test-bucket/test-key?versionId=test-versionId',
    ];
    retryPaths.forEach(path => {
        it(`should get a 200 response for route: GET ${path}`, done => {
            const url = getUrl(path);

            http.get(url, res => {
                assert.equal(res.statusCode, 200);
                done();
            });
        });
    });

    const retryQueryPaths = [
        '/_/crr/failed?marker=foo',
        '/_/crr/failed?marker=',
        '/_/crr/failed?sitename=',
    ];

    retryQueryPaths.forEach(path => {
        it(`should get a 400 response for route: ${path}`, done => {
            const url = getUrl(path);

            http.get(url, res => {
                assert.equal(res.statusCode, 400);
                done();
            });
        });
    });

    it('should get a 200 response for route: /_/crr/failed', done => {
        const member = [getMember(`test-bucket:test-key:${testVersionId}`)];
        addMembers(redisClient, 'test-site', member, err => {
            assert.ifError(err);
            const body = JSON.stringify([{
                Bucket: 'test-bucket',
                Key: 'test-key',
                VersionId: 'test-versionId',
                StorageClass: 'test-site',
            }]);
            return makeRetryPOSTRequest(body, (err, res) => {
                assert.ifError(err);
                assert.strictEqual(res.statusCode, 200);
                done();
            });
        });
    });

    const invalidPOSTRequestBodies = [
        'a',
        {},
        [],
        ['a'],
        [{
            // Missing Bucket property.
            Key: 'b',
            VersionId: 'c',
            StorageClass: 'd',
        }],
        [{
            // Missing Key property.
            Bucket: 'a',
            VersionId: 'c',
            StorageClass: 'd',
        }],
        [{
            // Missing StorageClass property.
            Bucket: 'a',
            Key: 'b',
            VersionId: 'c',
        }],
    ];

    if (useRoleAuth) {
        invalidPOSTRequestBodies.forEach(body => {
            if (typeof body === 'object' && body[0]) {
                // eslint-disable-next-line no-param-reassign
                body[0].Role = 'e';
            }
        });
        invalidPOSTRequestBodies.push([{
            // Missing Role property.
            Bucket: 'a',
            Key: 'b',
            VersionId: 'c',
            StorageClass: 'd',
        }]);
    }

    invalidPOSTRequestBodies.forEach(body => {
        const invalidBody = JSON.stringify(body);
        it('should get a 400 response for route: /_/crr/failed when ' +
        `given an invalid request body: ${invalidBody}`, done => {
            makeRetryPOSTRequest(invalidBody, (err, res) => {
                assert.strictEqual(res.statusCode, 400);
                return getResponseBody(res, (err, resBody) => {
                    assert.ifError(err);
                    const body = JSON.parse(resBody);
                    assert(body.MalformedPOSTRequest);
                    return done();
                });
            });
        });
    });
});

describe('CRR Retry feature', () => {
    before(done => {
        const S3Mock = require('../utils/S3Mock');
        const VaultMock = require('../utils/VaultMock');
        const s3Mock = new S3Mock();
        const vaultMock = new VaultMock();
        http.createServer((req, res) => s3Mock.onRequest(req, res))
            .listen(config.extensions.replication.source.s3.port);
        http.createServer((req, res) => vaultMock.onRequest(req, res))
            .listen(config.extensions.replication.source.auth.vault.port);
        deleteKeys(redisClient, done);
    });

    afterEach(done => deleteKeys(redisClient, done));

    it('should get correct data for GET route: /_/crr/failed when no ' +
    'key has been created', done => {
        getRequest('/_/crr/failed', (err, res) => {
            assert.ifError(err);
            assert.deepStrictEqual(res, {
                IsTruncated: false,
                Versions: [],
            });
            done();
        });
    });

    it('should return empty array for route: /_/crr/failed when ' +
    'no sitename is given', done => {
        const member = getMember(`test-bucket-1:test-key:${testVersionId}`);
        addMembers(redisClient, 'test-site', [member], err => {
            assert.ifError(err);
            getRequest('/_/crr/failed', (err, res) => {
                assert.ifError(err);
                assert.deepStrictEqual(res, {
                    IsTruncated: false,
                    Versions: [],
                });
                done();
            });
        });
    });

    it('should get correct data for GET route: /_/crr/failed when ' +
    'the key has been created and there is one key', done => {
        const member = getMember(`test-bucket-1:test-key:${testVersionId}`);
        addMembers(redisClient, 'test-site', [member], err => {
            assert.ifError(err);
            getRequest('/_/crr/failed?marker=0&sitename=test-site',
            (err, res) => {
                assert.ifError(err);
                const Versions = addRoleProp([{
                    Bucket: 'test-bucket-1',
                    Key: 'test-key',
                    VersionId: testVersionId,
                    StorageClass: 'test-site',
                    Size: 1,
                    LastModified: '2018-03-30T22:22:34.384Z',
                }]);
                assert.deepStrictEqual(res, {
                    IsTruncated: false,
                    Versions,
                });
                done();
            });
        });
    });

    it('should get correct data for GET route: /_/crr/failed when ' +
    'unknown marker is given',
    done => {
        const site = 'test-site';
        const member = getMember(`test-bucket-1:test-key:${testVersionId}`);
        addMembers(redisClient, site, [member], err => {
            assert.ifError(err);
            const endpoint = `/_/crr/failed?marker=9999999999&sitename=${site}`;
            return getRequest(endpoint, (err, res) => {
                assert.ifError(err);
                assert.deepStrictEqual(res, {
                    IsTruncated: false,
                    Versions: [],
                });
                done();
            });
        });
    });

    it('should get correct data for GET route: /_/crr/failed and ' +
    'normalize members when there are duplicate failures across hours',
    done => {
        const hour = 60 * 60 * 1000;
        const oneHoursAgo = new Date(Date.now() - hour);
        const twoHoursAgo = new Date(Date.now() - (hour * 2));
        const statsClient = new StatsModel(redisClient);
        const norm1 = statsClient.normalizeTimestampByHour(oneHoursAgo);
        const norm2 = statsClient.normalizeTimestampByHour(twoHoursAgo);
        const site = 'test-site';
        const bucket = 'test-bucket';
        const objectKey = 'test-key';
        const member = getMember(`${bucket}:${objectKey}:${testVersionId}`);
        return async.series([
            next => addManyMembers(redisClient, site, [member], norm1,
                norm1 + 1, next),
            next => addManyMembers(redisClient, site, [member], norm2,
                norm2 + 2, next),
            next =>
                getRequest(`/_/crr/failed?marker=0&sitename=${site}`,
                    (err, res) => {
                        assert.ifError(err);
                        const Versions = addRoleProp([{
                            Bucket: bucket,
                            Key: objectKey,
                            VersionId: testVersionId,
                            StorageClass: site,
                            Size: 1,
                            LastModified: '2018-03-30T22:22:34.384Z',
                        }]);
                        assert.deepStrictEqual(res, {
                            IsTruncated: false,
                            Versions,
                        });
                        return next();
                    }),
            next => setTimeout(() => {
                async.map([
                    `${TEST_REDIS_KEY_FAILED_CRR}:${site}:${norm1}`,
                    `${TEST_REDIS_KEY_FAILED_CRR}:${site}:${norm2}`,
                ],
                (key, cb) => redisClient.zcard(key, cb),
                (err, results) => {
                    assert.ifError(err);
                    const sum = results[0] + results[1];
                    assert.strictEqual(sum, 1);
                    return next();
                });
            }, 2000),
        ], done);
    });

    it('should get correct data for GET route: /_/crr/failed when ' +
    'the key has been created and there are multiple members',
    done => {
        const members = [
            getMember('test-bucket:test-key:test-versionId'),
            getMember('test-bucket-1:test-key-1:test-versionId-1'),
            getMember('test-bucket-2:test-key-2:test-versionId-2'),
        ];
        addMembers(redisClient, 'test-site', members, err => {
            assert.ifError(err);
            getRequest('/_/crr/failed?marker=0&sitename=test-site',
            (err, res) => {
                assert.ifError(err);
                assert.strictEqual(res.IsTruncated, false);
                assert.strictEqual(res.Versions.length, 3);
                // We cannot guarantee order because it depends on how
                // Redis fetches the keys.
                members.forEach(member => {
                    // eslint-disable-next-line no-unused-vars
                    const [bucket, key, versionId] = member.split(':');
                    assert(res.Versions.some(o => (
                        o.Bucket === bucket &&
                        o.Key === key &&
                        o.VersionId === testVersionId &&
                        o.StorageClass === 'test-site'
                    )));
                });
                done();
            });
        });
    });

    it('should get correct data for GET route: /_/crr/failed when ' +
    'the two site keys have been created',
    done => {
        const members = [
            getMember('test-bucket:test-key:test-versionId'),
            getMember('test-bucket-1:test-key-1:test-versionId-1'),
            getMember('test-bucket-2:test-key-2:test-versionId-2'),
        ];
        async.series([
            next => addMembers(redisClient, 'test-site', members, next),
            next => addMembers(redisClient, 'test-site-1', members, next),
            next => getRequest('/_/crr/failed?sitename=test-site',
                (err, res) => {
                    assert.ifError(err);
                    assert.strictEqual(res.IsTruncated, false);
                    assert.strictEqual(res.Versions.length, 3);
                    members.forEach(member => {
                        // eslint-disable-next-line no-unused-vars
                        const [bucket, key, versionId] = member.split(':');
                        assert(res.Versions.some(o => (
                            o.Bucket === bucket &&
                            o.Key === key &&
                            o.VersionId === testVersionId &&
                            o.StorageClass === 'test-site'
                        )));
                    });
                    next();
                }),
            next => getRequest('/_/crr/failed?sitename=test-site-1',
                (err, res) => {
                    assert.ifError(err);
                    assert.strictEqual(res.IsTruncated, false);
                    assert.strictEqual(res.Versions.length, 3);
                    members.forEach(member => {
                        // eslint-disable-next-line no-unused-vars
                        const [bucket, key, versionId] = member.split(':');
                        assert(res.Versions.some(o => (
                            o.Bucket === bucket &&
                            o.Key === key &&
                            o.VersionId === testVersionId &&
                            o.StorageClass === 'test-site-1'
                        )));
                    });
                    next();
                }),
        ], done);
    });

    it('should get correct data at scale for GET route: ' +
    '/_/crr/failed when failures occur across hours',
    function f(done) {
        this.timeout(300000);
        const hours = Array.from(Array(24).keys());
        async.eachLimit(hours, 10, (hour, callback) => {
            const delta = (60 * 60 * 1000) * hour;
            let epoch = Date.now() - delta;
            return async.timesLimit(150, 10, (i, next) => {
                const members = [
                    getMember(`bucket-${i}:key-a-${i}-${hour}:versionId-${i}`),
                    getMember(`bucket-${i}:key-b-${i}-${hour}:versionId-${i}`),
                    getMember(`bucket-${i}:key-c-${i}-${hour}:versionId-${i}`),
                    getMember(`bucket-${i}:key-d-${i}-${hour}:versionId-${i}`),
                    getMember(`bucket-${i}:key-e-${i}-${hour}:versionId-${i}`),
                ];
                const statsClient = new StatsModel(redisClient);
                const normalizedScore = statsClient
                    .normalizeTimestampByHour(new Date(epoch));
                epoch += 5;
                return addManyMembers(redisClient, 'test-site', members,
                    normalizedScore, epoch, next);
            }, callback);
        }, err => {
            assert.ifError(err);
            const memberCount = (150 * 5) * 24;
            const set = new Set();
            let marker = 0;
            async.timesSeries(memberCount, (i, next) =>
                getRequest(`/_/crr/failed?marker=${marker}&sitename=test-site`,
                    (err, res) => {
                        assert.ifError(err);
                        res.Versions.forEach(version => {
                            // Ensure we have no duplicate results.
                            assert(!set.has(version.Key));
                            set.add(version.Key);
                        });
                        if (res.IsTruncated === false) {
                            assert.strictEqual(res.NextMarker, undefined);
                            assert.strictEqual(set.size, memberCount);
                            return done();
                        }
                        assert.strictEqual(res.IsTruncated, true);
                        assert.strictEqual(typeof(res.NextMarker), 'number');
                        marker = res.NextMarker;
                        return next();
                    }), done);
        });
    });

    it('should get correct data at scale for GET route: ' +
    '/_/crr/failed when failures occur in the same hour',
    function f(done) {
        this.timeout(30000);
        const statsClient = new StatsModel(redisClient);
        const epoch = Date.now();
        let twelveHoursAgo = epoch - (60 * 60 * 1000) * 12;
        const normalizedScore = statsClient
            .normalizeTimestampByHour(new Date(twelveHoursAgo));
        return async.timesLimit(2000, 10, (i, next) => {
            const members = [
                getMember(`bucket-${i}:key-a-${i}:versionId-${i}`),
                getMember(`bucket-${i}:key-b-${i}:versionId-${i}`),
                getMember(`bucket-${i}:key-c-${i}:versionId-${i}`),
                getMember(`bucket-${i}:key-d-${i}:versionId-${i}`),
                getMember(`bucket-${i}:key-e-${i}:versionId-${i}`),
            ];
            twelveHoursAgo += 5;
            return addManyMembers(redisClient, 'test-site', members,
                normalizedScore, twelveHoursAgo, next);
        }, err => {
            assert.ifError(err);
            const memberCount = 2000 * 5;
            const set = new Set();
            let marker = 0;
            async.timesSeries(memberCount, (i, next) =>
                getRequest(`/_/crr/failed?marker=${marker}&sitename=test-site`,
                    (err, res) => {
                        assert.ifError(err);
                        res.Versions.forEach(version => {
                            // Ensure we have no duplicate results.
                            assert(!set.has(version.Key));
                            set.add(version.Key);
                        });
                        if (res.IsTruncated === false) {
                            assert.strictEqual(res.NextMarker, undefined);
                            assert.strictEqual(set.size, memberCount);
                            return done();
                        }
                        assert.strictEqual(res.IsTruncated, true);
                        assert.strictEqual(typeof(res.NextMarker), 'number');
                        marker = res.NextMarker;
                        return next();
                    }), done);
        });
    });

    it('should get correct data for GET route: ' +
    '/_/crr/failed/<bucket>/<key>/<versionId> when there is no key',
    done => {
        getRequest('/_/crr/failed/test-bucket/test-key/test-versionId',
        (err, res) => {
            assert.ifError(err);
            assert.deepStrictEqual(res, {
                IsTruncated: false,
                Versions: [],
            });
            done();
        });
    });

    it('should get correct data for GET route: ' +
    '/_/crr/failed/<bucket>/<key>?versionId=<versionId>', done => {
        const member = getMember('test-bucket:test-key/a:test-versionId');
        const member1 = getMember('test-bucket:test-key-1/a:test-versionId');
        async.series([
            next => addMembers(redisClient, 'test-site-2', [member], next),
            next => addMembers(redisClient, 'test-site-2', [member1], next),
            next => addMembers(redisClient, 'test-site-1', [member], next),
            next => {
                let route = '/_/crr/failed/test-bucket/test-key/a?' +
                    'versionId=test-versionId';
                if (useRoleAuth) {
                    route += `&role=${testRole}`;
                }
                return getRequest(route, (err, res) => {
                    assert.ifError(err);
                    assert.strictEqual(res.IsTruncated, false);
                    assert.strictEqual(res.Versions.length, 2);
                    const matches = [{
                        member,
                        site: 'test-site-2',
                    }, {
                        member,
                        site: 'test-site-1',
                    }];
                    matches.forEach(match => {
                        const { member, site } = match;
                        // eslint-disable-next-line no-unused-vars
                        const [bucket, key, versionId] =
                            member.split(':');
                        assert(res.Versions.some(o => (
                            o.Bucket === bucket &&
                            o.Key === key &&
                            o.VersionId === testVersionId &&
                            o.StorageClass === site
                        )));
                    });
                    return next();
                });
            },
        ], err => {
            assert.ifError(err);
            return done();
        });
    });

    it('should get correct data for POST route: ' +
    '/_/crr/failed when no key has been matched', done => {
        const requestBody = addRoleProp([{
            Bucket: 'bucket',
            Key: 'key',
            VersionId: 'versionId',
            StorageClass: 'site',
        }]);
        const body = JSON.stringify(requestBody);
        makeRetryPOSTRequest(body, (err, res) => {
            assert.ifError(err);
            getResponseBody(res, (err, resBody) => {
                assert.ifError(err);
                const body = JSON.parse(resBody);
                assert.deepStrictEqual(body, []);
                done();
            });
        });
    });

    it('should get correct data for POST route: ' +
    '/_/crr/failed when no key has been matched and ForceRetry ' +
    'field is specified', done => {
        const body = JSON.stringify([{
            Bucket: 'bucket',
            Key: 'key',
            VersionId: 'versionId',
            StorageClass: 'site',
            ForceRetry: true,
        }]);
        makeRetryPOSTRequest(body, (err, res) => {
            assert.ifError(err);
            getResponseBody(res, (err, resBody) => {
                assert.ifError(err);
                const body = JSON.parse(resBody);
                assert.deepStrictEqual(body, [{
                    Bucket: 'bucket',
                    Key: 'key',
                    VersionId: testVersionId,
                    StorageClass: 'site',
                    ReplicationStatus: 'PENDING',
                    LastModified: '2018-03-30T22:22:34.384Z',
                    Size: 1,
                }]);
                done();
            });
        });
    }).timeout(10000);

    it('should get correct data for POST route: /_/crr/failed ' +
    'when there are multiple matching keys', done => {
        const member = getMember(`test-bucket:test-key:${testVersionId}`);
        async.series([
            next => addMembers(redisClient, 'test-site-1', [member], next),
            next => addMembers(redisClient, 'test-site-2', [member], next),
            next => addMembers(redisClient, 'test-site-3', [member], next),
        ], err => {
            assert.ifError(err);
            const requestBody = addRoleProp([{
                Bucket: 'test-bucket',
                Key: 'test-key',
                VersionId: testVersionId,
                StorageClass: 'test-site-1',
            }, {
                Bucket: 'test-bucket',
                Key: 'test-key',
                VersionId: testVersionId,
                // Should not be in response.
                StorageClass: 'test-site-unknown',
            }, {
                Bucket: 'test-bucket',
                Key: 'test-key',
                VersionId: testVersionId,
                StorageClass: 'test-site-2',
            }, {
                Bucket: 'test-bucket',
                Key: 'test-key',
                VersionId: testVersionId,
                StorageClass: 'test-site-3',
            }]);
            const body = JSON.stringify(requestBody);
            makeRetryPOSTRequest(body, (err, res) => {
                assert.ifError(err);
                getResponseBody(res, (err, resBody) => {
                    assert.ifError(err);
                    const body = JSON.parse(resBody);
                    const Versions = addRoleProp([{
                        Bucket: 'test-bucket',
                        Key: 'test-key',
                        VersionId: testVersionId,
                        StorageClass: 'test-site-1',
                        ReplicationStatus: 'PENDING',
                        LastModified: '2018-03-30T22:22:34.384Z',
                        Size: 1,
                    }, {
                        Bucket: 'test-bucket',
                        Key: 'test-key',
                        VersionId: testVersionId,
                        StorageClass: 'test-site-2',
                        ReplicationStatus: 'PENDING',
                        LastModified: '2018-03-30T22:22:34.384Z',
                        Size: 1,
                    }, {
                        Bucket: 'test-bucket',
                        Key: 'test-key',
                        VersionId: testVersionId,
                        StorageClass: 'test-site-3',
                        ReplicationStatus: 'PENDING',
                        LastModified: '2018-03-30T22:22:34.384Z',
                        Size: 1,
                    }]);
                    assert.deepStrictEqual(body, Versions);
                    done();
                });
            });
        });
    }).timeout(10000);

    it('should get correct data at scale for POST route: /_/crr/failed',
    function f(done) {
        this.timeout(30000);
        const reqBody = [];
        async.timesLimit(10, 10, (i, next) => {
            const Versions = addRoleProp([{
                Bucket: `bucket-${i}`,
                Key: `key-${i}`,
                VersionId: testVersionId,
                StorageClass: `site-${i}-a`,
            }, {
                Bucket: `bucket-${i}`,
                Key: `key-${i}`,
                VersionId: testVersionId,
                StorageClass: `site-${i}-b`,
            }, {
                Bucket: `bucket-${i}`,
                Key: `key-${i}`,
                VersionId: testVersionId,
                StorageClass: `site-${i}-c`,
            }, {
                Bucket: `bucket-${i}`,
                Key: `key-${i}`,
                VersionId: testVersionId,
                StorageClass: `site-${i}-d`,
            }, {
                Bucket: `bucket-${i}`,
                Key: `key-${i}`,
                VersionId: testVersionId,
                StorageClass: `site-${i}-e`,
            }]);
            reqBody.push(...Versions);
            const member = getMember(`bucket-${i}:key-${i}:${testVersionId}`);
            async.series([
                next => addMembers(redisClient, `site-${i}-a`, [member], next),
                next => addMembers(redisClient, `site-${i}-b`, [member], next),
                next => addMembers(redisClient, `site-${i}-c`, [member], next),
                next => addMembers(redisClient, `site-${i}-d`, [member], next),
                next => addMembers(redisClient, `site-${i}-e`, [member], next),
            ], next);
        }, err => {
            assert.ifError(err);
            const body = JSON.stringify(reqBody);
            assert.ifError(err);
            makeRetryPOSTRequest(body, (err, res) => {
                assert.ifError(err);
                getResponseBody(res, (err, resBody) => {
                    assert.ifError(err);
                    const body = JSON.parse(resBody);
                    assert.strictEqual(body.length, 10 * 5);
                    done();
                });
            });
        });
    });
});
