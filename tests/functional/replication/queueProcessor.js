const assert = require('assert');
const crypto = require('crypto');
const http = require('http');
const URL = require('url');
const querystring = require('querystring');

const VersionIDUtils = require('arsenal').versioning.VersionID;

const QueueProcessor = require('../../../extensions/replication' +
                               '/queueProcessor/QueueProcessor');

/* eslint-disable max-len */

const constants = {
    source: {
        s3: '127.0.0.1',
        vault: '127.0.0.2',
        bucket: 'source-bucket',
        accountId: 123456789012,
        accessKey: 'accessKey1',
        secretKey: 'verySecretKey1',
        canonicalId: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        dataKey: '6d808697fbaf9f16fb32b94be189b80b3b9b2890',
    },
    target: {
        s3: '127.0.0.3',
        vault: '127.0.0.4',
        bucket: 'target-bucket',
        accountId: 123456789013,
        accessKey: 'accessKey2',
        secretKey: 'verySecretKey2',
        canonicalId: 'bac4bea1bac4bea1bac4bea1bac4bea1bac4bea1bac4bea1bac4bea1bac4bea1',
        dataKey: '7d808697fbaf9f16fb32b94be189b80b3b9b2890',
    },
    key: 'key_to_replicate',
    versionId: '98498980852335999999RG001  100',
    body: 'some contents to be replicated',
    roleSessionName: 'backbeat-replication',
};

class S3Mock {
    constructor() {
        this.versionIdEncoded = VersionIDUtils.encode(constants.versionId);
        this.contentMd5 = crypto.createHash('md5')
            .update(constants.body).digest('hex');
        this.sourceRole =
            `arn:aws:iam::${constants.source.accountId}:role/backbeat`;
        this.targetRole =
            `arn:aws:iam::${constants.target.accountId}:role/backbeat`;
        this.kafkaEntry = {
            key: 'somekey',
            value: JSON.stringify({
                type: 'put',
                bucket: constants.source.bucket,
                key: `${constants.key}\u0000${constants.versionId}`,
                value: JSON.stringify({
                    'md-model-version': 2,
                    'owner-display-name': 'Bart',
                    'owner-id': constants.source.canonicalId,
                    'content-length': constants.body.length,
                    'content-type': 'text/plain',
                    'last-modified': '2017-07-25T21:45:47.660Z',
                    'content-md5': this.contentMd5,
                    'x-amz-version-id': 'null',
                    'x-amz-server-version-id': '',
                    'x-amz-storage-class': 'STANDARD',
                    'x-amz-server-side-encryption': '',
                    'x-amz-server-side-encryption-aws-kms-key-id': '',
                    'x-amz-server-side-encryption-customer-algorithm': '',
                    'x-amz-website-redirect-location': '',
                    'acl': {
                        Canned: 'private',
                        FULL_CONTROL: [],
                        WRITE_ACP: [],
                        READ: [],
                        READ_ACP: [],
                    },
                    'key': '',
                    'location': [{
                        key: constants.source.dataKey,
                        size: constants.body.length,
                        start: 0,
                        dataStoreName: 'file',
                    }],
                    'isDeleteMarker': false,
                    'tags': {},
                    'replicationInfo': {
                        status: 'PENDING',
                        content: ['DATA', 'METADATA'],
                        destination: `arn:aws:s3:::${constants.target.bucket}`,
                        storageClass: 'STANDARD',
                        role: `${this.sourceRole},${this.targetRole}`,
                    },
                    'x-amz-meta-s3cmd-attrs': `uid:0/gname:root/uname:root/gid:0/mode:33188/mtime:1501018866/atime:1501018885/md5:${this.contentMd5}/ctime:1501018866`,
                    'versionId': constants.versionId,
                }),
            }),
        };
        this.replicatedData = null;
        this.replicatedMd = null;

        this.baselineHandlers = [
            {
                host: constants.source.s3,
                method: 'GET',
                path: `/${constants.source.bucket}`,
                query: {
                    replication: '',
                },
                handler: (req, url, query, res) => {
                    res.setHeader('content-type', 'application/xml');
                    res.writeHead(200);
                    res.end(`<?xml version="1.0" encoding="UTF-8"?><ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>myrule</ID><Prefix/><Status>Enabled</Status><Destination><Bucket>arn:aws:s3:::${constants.target.bucket}</Bucket><StorageClass>STANDARD</StorageClass></Destination></Rule><Role>${this.sourceRole},${this.targetRole}</Role></ReplicationConfiguration>`);
                },

            }, {
                host: constants.target.vault,
                method: 'GET',
                path: '/',
                query: {
                    Action: 'AccountsCanonicalIds',
                },
                handler: (req, url, query, res) => {
                    assert.strictEqual(Number(query.accountIds),
                                       constants.target.accountId);

                    res.writeHead(200);
                    res.end(JSON.stringify([
                        {
                            accountId: query.accountId,
                            canonicalId: constants.target.canonicalId,
                        },
                    ]));
                },

            }, {
                host: constants.source.vault,
                method: 'POST',
                path: '/',
                query: {
                    Action: 'AssumeRoleBackbeat',
                },
                handler: (req, url, query, res) => {
                    assert.strictEqual(query.RoleArn, this.sourceRole);
                    assert.strictEqual(query.RoleSessionName,
                                       constants.roleSessionName);

                    res.setHeader('content-type', 'application/json');
                    res.writeHead(200);
                    res.end(JSON.stringify({
                        Credentials: {
                            AccessKeyId: constants.source.accessKey,
                            SecretAccessKey: constants.source.secretKey,
                            SessionToken: 'dummySessionToken',
                            Expiration: 1501108900014,
                        },
                        AssumedRoleUser: 'arn:aws:sts::123456789012:assumed-role/backbeat/backbeat-replication',
                    }));
                },

            }, {
                host: constants.source.s3,
                method: 'GET',
                path: `/${constants.source.bucket}/${constants.key}`,
                query: {},
                handler: (req, url, query, res) => {
                    assert.strictEqual(query.versionId,
                                       this.versionIdEncoded);

                    res.setHeader('content-type', 'application/octet-stream');
                    res.setHeader('content-length', constants.body.length);
                    res.writeHead(200);
                    res.end(constants.body);
                },

            }, {
                host: constants.target.vault,
                method: 'POST',
                path: '/',
                query: {
                    Action: 'AssumeRoleBackbeat',
                },
                handler: (req, url, query, res) => {
                    assert.strictEqual(query.RoleArn, this.targetRole);
                    assert.strictEqual(query.RoleSessionName,
                                       constants.roleSessionName);

                    res.setHeader('content-type', 'application/json');
                    res.writeHead(200);
                    res.end(JSON.stringify({
                        Credentials: {
                            AccessKeyId: constants.target.accessKey,
                            SecretAccessKey: constants.target.secretKey,
                            SessionToken: 'dummySessionToken',
                            Expiration: 1501108900014,
                        },
                        AssumedRoleUser: `arn:aws:sts::${constants.source.accountId}:assumed-role/backbeat/${constants.roleSessionName}`,
                    }));
                },

            }, {
                host: constants.target.s3,
                method: 'PUT',
                path: `/_/backbeat/${constants.target.bucket}/${constants.key}/data`,
                query: {},
                handler: (req, url, query, reqBody, res) => {
                    assert.strictEqual(this.hasPutTargetData, false);
                    assert.strictEqual(reqBody, constants.body);

                    res.setHeader('content-type', 'application/json');
                    res.writeHead(200);
                    res.end(JSON.stringify([
                        { key: constants.target.dataKey,
                          start: 0,
                          size: constants.body.length,
                          dataStoreName: 'file',
                        },
                    ]));
                    this.hasPutTargetData = true;
                },

            }, {
                host: constants.target.s3,
                method: 'PUT',
                path: `/_/backbeat/${constants.target.bucket}/${constants.key}/metadata`,
                query: {},
                handler: (req, url, query, reqBody, res) => {
                    assert.strictEqual(this.hasPutTargetData, true);
                    assert.strictEqual(this.hasPutTargetMd, false);

                    const parsedMd = JSON.parse(reqBody);
                    assert.deepStrictEqual(parsedMd.replicationInfo, {
                        status: 'REPLICA',
                        content: ['DATA', 'METADATA'],
                        destination: `arn:aws:s3:::${constants.target.bucket}`,
                        storageClass: 'STANDARD',
                        role: `${this.sourceRole},${this.targetRole}`,
                    });
                    assert.strictEqual(parsedMd['owner-id'],
                                       constants.target.canonicalId);
                    assert.deepStrictEqual(parsedMd.location, [
                        { key: constants.target.dataKey,
                          start: 0,
                          size: constants.body.length,
                          dataStoreName: 'file',
                        },
                    ]);

                    res.writeHead(200);
                    res.end();
                    this.hasPutTargetMd = true;
                },

            }, {
                host: constants.source.s3,
                method: 'PUT',
                path: `/_/backbeat/${constants.source.bucket}/${constants.key}/metadata`,
                query: {},
                handler: (req, url, query, reqBody, res) => {
                    assert.strictEqual(this.hasPutTargetMd, true);
                    assert.strictEqual(this.hasPutSourceMd, false);

                    const parsedMd = JSON.parse(reqBody);
                    assert.deepStrictEqual(parsedMd.replicationInfo, {
                        status: 'COMPLETED',
                        content: ['DATA', 'METADATA'],
                        destination: `arn:aws:s3:::${constants.target.bucket}`,
                        storageClass: 'STANDARD',
                        role: `${this.sourceRole},${this.targetRole}`,
                    });
                    assert.strictEqual(parsedMd['owner-id'],
                                       constants.source.canonicalId);

                    res.writeHead(200);
                    res.end();
                    this.hasPutSourceMd = true;
                },
            },
        ];

        this.resetTest();
    }

    resetTest() {
        this.hasPutTargetData = false;
        this.hasPutTargetMd = false;
        this.hasPutSourceMd = false;
    }

    _matchHandler(req, url, query) {
        return this.baselineHandlers.find(
            h => (h.host === req.headers.host.split(':')[0] &&
                  h.method === req.method &&
                  h.path === url.pathname &&
                  Object.keys(h.query).every(k => query[k] === h.query[k])));
    }

    _onParsedRequest(req, url, query, reqBody, res) {
        const handler = this._matchHandler(req, url, query);
        if (handler !== undefined) {
            if (req.method === 'GET') {
                return handler.handler(req, url, query, res);
            }
            if (req.method === 'PUT') {
                return handler.handler(req, url, query, reqBody, res);
            }
            if (req.method === 'POST') {
                return handler.handler(req, url, query, res);
            }
        }
        res.writeHead(501);
        return res.end(JSON.stringify({
            error: 'mock not implemented',
            method: req.method,
            url: req.url,
            host: req.headers.host,
        }));
    }

    onRequest(req, res) {
        const url = URL.parse(req.url);
        const query = querystring.parse(url.query);
        if (req.method === 'PUT' || req.method === 'POST') {
            const chunks = [];
            req.on('data', chunk => chunks.push(chunk));
            return req.on('end', () => {
                const reqBody = Buffer.concat(chunks).toString();
                if (req.method === 'POST') {
                    const formData = querystring.parse(reqBody);
                    return this._onParsedRequest(req, url, formData,
                                                 reqBody, res);
                }
                return this._onParsedRequest(req, url, query, reqBody, res);
            });
        }
        return this._onParsedRequest(req, url, query, null, res);
    }
}

/* eslint-enable max-len */

describe('queue processor error management with mocking', () => {
    let queueProcessor;
    let httpServer;
    let s3mock;

    before(() => {
        queueProcessor = new QueueProcessor(
            {} /* zkConfig not needed */,
            { auth: { type: 'role',
                      vault: { host: constants.source.vault,
                               port: 7777 } },
              s3: { host: constants.source.s3,
                    port: 7777, transport: 'http' } },
            { auth: { type: 'role',
                      vault: { host: constants.target.vault,
                               port: 7777 } },
              s3: { host: constants.target.s3,
                    port: 7777, transport: 'http' } },
            {} /* repConfig not needed */,
            { logLevel: 'info', dumpLevel: 'error' });

        // don't call start() on the queue processor, so that we don't
        // attempt to fetch entries from kafka

        s3mock = new S3Mock();
        httpServer = http.createServer(
            (req, res) => s3mock.onRequest(req, res));
        httpServer.listen(7777);
    });

    afterEach(() => {
        s3mock.resetTest();
    });

    after(() => {
        httpServer.close();
    });

    it('should complete a replication end-to-end', done => {
        queueProcessor.processKafkaEntry(s3mock.kafkaEntry, err => {
            assert.ifError(err);
            assert(s3mock.hasPutTargetData);
            assert(s3mock.hasPutTargetMd);
            assert(s3mock.hasPutSourceMd);
            done();
        });
    });
});
