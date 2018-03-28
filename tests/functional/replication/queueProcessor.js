const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const http = require('http');
const URL = require('url');
const querystring = require('querystring');

const VersionIDUtils = require('arsenal').versioning.VersionID;
const routesUtils = require('arsenal').s3routes.routesUtils;
const errors = require('arsenal').errors;

const werelogs = require('werelogs');
const Logger = werelogs.Logger;

const QueueProcessor = require('../../../extensions/replication' +
                               '/queueProcessor/QueueProcessor');
const ReplicationStatusProcessor =
          require('../../../extensions/replication' +
                  '/replicationStatusProcessor/ReplicationStatusProcessor');
const TestConfigurator = require('../../utils/TestConfigurator');

/* eslint-disable max-len */


function getMD5(body) {
    return crypto.createHash('md5').update(body).digest('hex');
}

function buildLocations(keysArray, bodiesArray, options) {
    const locations = [];
    let start = 0;
    for (let i = 0; i < keysArray.length; ++i) {
        const location = { key: keysArray[i],
            start,
            size: bodiesArray[i].length,
            dataStoreName: 'file' };
        if (!(options && options.doNotIncludeETag)) {
            location.dataStoreETag = `${i + 1}:${getMD5(bodiesArray[i])}`;
        }
        locations.push(location);
        start += bodiesArray[i].length;
    }
    return (locations.length > 0 ? locations : null);
}

const XML_CHARACTER_MAP = {
    '&': '&amp;',
    '"': '&quot;',
    "'": '&apos;',
    '<': '&lt;',
    '>': '&gt;',
};

const xmlRegex = new RegExp(
    `[${Object.keys(XML_CHARACTER_MAP).join('')}]`, 'g');

function escapeForXML(string) {
    return string && string.replace
        ? string.replace(xmlRegex, item => XML_CHARACTER_MAP[item])
        : string;
}

const constants = {
    source: {
        s3: '127.0.0.1',
        vault: '127.0.0.2',
        dataPartsKeys: ['6d808697fbaf9f16fb32b94be189b80b3b9b2890',
            'ab30293a044eca5215068c6a06cfdb1b636a16e4'],
    },
    target: {
        hosts: [{ host: '127.0.0.3', port: 7777 },
                { host: '127.0.0.4', port: 7777 }],
        dataPartsKeys: ['7d808697fbaf9f16fb32b94be189b80b3b9b2890',
            'e54e2ced6625f67e07f4735fb7b897a7bc81d603'],
    },
    partsContents: ['some contents to be replicated',
        'some more contents to be replicated'],
};

class S3Mock extends TestConfigurator {
    constructor() {
        super();

        this.log = new Logger('QueueProcessor:test:S3Mock');

        const sourceMd = {
            versionedKey: () =>
                `${this.getParam('key')}\u0000${this.getParam('versionId')}`,
            contentMd5: () => this.getParam('contentMd5'),
            contentLength: () => this.getParam('contentLength'),
            location: () =>
                buildLocations(this.getParam('source.dataPartsKeys'),
                               this.getParam('partsContents')),
            replicationInfo: {
                role: () =>
                    `${this.getParam('source.role')},${this.getParam('target.role')}`,
                destination: () =>
                    `arn:aws:s3:::${this.getParam('target.bucket')}`,
                content: ['DATA', 'METADATA'],
            },
        };

        const kafkaEntry = () => ({
            key: 'somekey',
            value: JSON.stringify({
                type: 'put',
                bucket: this.getParam('source.bucket'),
                key: this.getParam('source.md.versionedKey'),
                site: 'sf',
                value: JSON.stringify({
                    'md-model-version': 2,
                    'owner-display-name': 'Bart',
                    'owner-id': this.getParam('source.canonicalId'),
                    'content-length': this.getParam('source.md.contentLength'),
                    'content-type': 'text/plain',
                    'last-modified': '2017-07-25T21:45:47.660Z',
                    'content-md5': this.getParam('source.md.contentMd5'),
                    'x-amz-version-id': 'null',
                    'x-amz-server-version-id': '',
                    'x-amz-storage-class': 'sf',
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
                    'location': this.getParam('source.md.location'),
                    'isDeleteMarker': false,
                    'tags': {},
                    'replicationInfo': {
                        status: 'PENDING',
                        backends: [{
                            site: 'sf',
                            status: 'PENDING',
                            dataStoreVersionId: '',
                        }, {
                            site: 'replicationaws',
                            status: 'PENDING',
                            dataStoreVersionId: '',
                        }],
                        content: this.getParam('source.md.replicationInfo.content'),

                        destination: this.getParam('source.md.replicationInfo.destination'),
                        storageClass: 'sf',
                        role: this.getParam('source.md.replicationInfo.role'),
                    },
                    'x-amz-meta-s3cmd-attrs': `uid:0/gname:root/uname:root/gid:0/mode:33188/mtime:1501018866/atime:1501018885/md5:${this.getParam('contentMd5')}/ctime:1501018866`,
                    'versionId': this.getParam('versionId'),
                }),
            }),
        });

        const routes = {
            source: {
                s3: {
                    getBucketReplication: () => ({
                        method: 'GET',
                        path: `/${this.getParam('source.bucket')}`,
                        query: {
                            replication: '',
                        },
                        handler: () => this._getBucketReplication,
                    }),
                    getObject: () => ({
                        method: 'GET',
                        path: `/${this.getParam('source.bucket')}/${this.getParam('encodedKey')}`,
                        query: {},
                        handler: () => this._getObject,
                    }),
                    putMetadata: () => ({
                        method: 'PUT',
                        path: `/_/backbeat/metadata/${this.getParam('source.bucket')}/${this.getParam('encodedKey')}`,
                        query: {},
                        handler: () => this._putMetadataSource,
                    }),
                    getMetadata: () => ({
                        method: 'GET',
                        path: `/_/backbeat/metadata/${this.getParam('source.bucket')}/${this.getParam('encodedKey')}`,
                        query: { versionId: VersionIDUtils.encode(this.getParam('versionId')) },
                        handler: () => this._getMetadataSource,
                    }),
                },
                vault: {
                    assumeRoleBackbeat: () => ({
                        method: 'POST',
                        path: '/',
                        query: {
                            Action: 'AssumeRoleBackbeat',
                        },
                        handler: () => this._assumeRoleBackbeatSource,
                    }),
                },
            },
            target: {
                putData: () => ({
                    method: 'PUT',
                    path: `/_/backbeat/data/${this.getParam('target.bucket')}/${this.getParam('encodedKey')}`,
                    query: {},
                    handler: () => this._putData,
                }),
                putMetadata: () => ({
                    method: 'PUT',
                    path: `/_/backbeat/metadata/${this.getParam('target.bucket')}/${this.getParam('encodedKey')}`,
                    query: {},
                    handler: () => this._putMetadataTarget,
                }),
                getAccountsCanonicalIds: () => ({
                    method: 'GET',
                    path: '/_/backbeat/vault',
                    query: {
                        Action: 'AccountsCanonicalIds',
                    },
                    handler: () => this._getAccountsCanonicalIds,
                }),
                assumeRoleBackbeat: () => ({
                    method: 'POST',
                    path: '/_/backbeat/vault',
                    query: {
                        Action: 'AssumeRoleBackbeat',
                    },
                    handler: () => this._assumeRoleBackbeatTarget,
                }),
            },
        };

        const params = {
            source: {
                bucket: 'source-bucket',
                accountId: 123456789012,
                accessKey: 'accessKey1',
                secretKey: 'verySecretKey1',
                canonicalId: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                dataPartsKeys: () =>
                    constants.source.dataPartsKeys.slice(
                        0, this.getParam('nbParts')),
                role: () =>
                    `arn:aws:iam::${this.getParam('source.accountId')}:role/backbeat`,
                assumedRole: () =>
                    `arn:aws:sts::${this.getParam('source.accountId')}:assumed-role/backbeat/${this.getParam('roleSessionName')}`,
                md: sourceMd,
            },
            target: {
                bucket: 'target-bucket',
                accountId: 123456789013,
                accessKey: 'accessKey2',
                secretKey: 'verySecretKey2',
                canonicalId: 'bac4bea7bac4bea7bac4bea7bac4bea7bac4bea7bac4bea7bac4bea7bac4bea7',
                dataPartsKeys: () =>
                    constants.target.dataPartsKeys.slice(
                        0, this.getParam('nbParts')),
                role: () =>
                    `arn:aws:iam::${this.getParam('target.accountId')}:role/backbeat`,
                assumedRole: () =>
                    `arn:aws:sts::${this.getParam('target.accountId')}:assumed-role/backbeat/${this.getParam('roleSessionName')}`,
                md: {
                    location: () =>
                        buildLocations(this.getParam('target.dataPartsKeys'),
                                       this.getParam('partsContents')),
                },
            },
            key: 'key_to_replicate_with_some_utf8_䆩鈁櫨㟔罳_and_encoded_chars_%EA%9D%8B',
            encodedKey: () => encodeURIComponent(this.getParam('key')),
            nbParts: 1,
            versionId: '98498980852335999999RG001  100',
            roleSessionName: 'backbeat-replication',
            replicationEnabled: true,
            partsContents: () =>
                constants.partsContents.slice(0, this.getParam('nbParts')),
            versionIdEncoded: () =>
                VersionIDUtils.encode(this.getParam('versionId')),
            contentLength: () =>
                this.getParam('partsContents')
                .reduce((sum, partBody) => sum + partBody.length, 0),
            contentMd5: () =>
                getMD5(this.getParam('partsContents').join('')),
            kafkaEntry,
            routes,
        };

        this.setParam(null, params, { persistent: true });

        this.resetTest();
    }

    resetTest() {
        super.resetTest();

        this.partsWritten = [];
        this.hasPutTargetData = false;
        this.putDataCount = 0;
        this.hasPutTargetMd = false;
        this.onPutSourceMd = null;
        this.setExpectedReplicationStatus('PROCESSING');
        this.requestsPerHost = {
            '127.0.0.1': 0,
            '127.0.0.2': 0,
            '127.0.0.3': 0,
            '127.0.0.4': 0,
        };
    }

    setExpectedReplicationStatus(expected) {
        this.expectedReplicationStatus = expected;
    }

    _findRouteHandler(req, url, query) {
        const host = req.headers.host.split(':')[0];
        let routesKey;
        if (host === constants.source.s3) {
            routesKey = 'routes.source.s3';
        } else if (host === constants.source.vault) {
            routesKey = 'routes.source.vault';
        } else if (constants.target.hosts.find(h => h.host === host)) {
            routesKey = 'routes.target';
        }
        const routes = this.getParam(routesKey);
        const action = Object.keys(routes).find(key => {
            const route = routes[key];
            return (route.method === req.method &&
                    route.path === url.pathname &&
                    Object.keys(route.query).every(
                        k => query[k] === route.query[k]));
        });
        if (action === undefined) {
            return undefined;
        }
        return this.getParam(`${routesKey}.${action}.handler`);
    }

    _onParsedRequest(req, url, query, res, reqBody) {
        const handler = this._findRouteHandler(req, url, query);
        if (handler !== undefined) {
            if (req.method === 'GET') {
                return handler.bind(this)(req, url, query, res);
            }
            if (req.method === 'PUT') {
                return handler.bind(this)(req, url, query, res, reqBody);
            }
            if (req.method === 'POST') {
                return handler.bind(this)(req, url, query, res);
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
        const host = req.headers.host.split(':')[0];
        this.requestsPerHost[host] += 1;

        if (req.method === 'PUT' || req.method === 'POST') {
            const chunks = [];
            req.on('data', chunk => chunks.push(chunk));
            return req.on('end', () => {
                const reqBody = Buffer.concat(chunks).toString();
                if (req.method === 'POST') {
                    const formData = querystring.parse(reqBody);
                    return this._onParsedRequest(req, url, formData,
                                                 res, reqBody);
                }
                return this._onParsedRequest(req, url, query, res, reqBody);
            });
        }
        return this._onParsedRequest(req, url, query, res);
    }

    installS3ErrorResponder(action, error, options) {
        this.setParam(`routes.${action}.handler`,
                      (req, url, query, res) => {
                          routesUtils.responseXMLBody(
                              error, null, res, this.log.newRequestLogger());
                          if (options && options.once) {
                              this.resetParam(`routes.${action}.handler`);
                          }
                      }, { _static: true });
    }

    installVaultErrorResponder(action, error, options) {
        const xmlBody = [
            '<ErrorResponse ',
            'xmlns="https://iam.amazonaws.com/doc/2010-05-08/">',
            '<Error>',
            '<Code>', error.message, '</Code>',
            '<Message>', escapeForXML(error.description), '</Message>',
            '</Error>',
            '<RequestId>42</RequestId>',
            '</ErrorResponse>',
        ];
        this.setParam(`routes.${action}.handler`,
                      (req, url, query, res) => {
                          res.writeHead(error.code);
                          res.end(xmlBody.join(''));
                          if (options && options.once) {
                              this.resetParam(`routes.${action}.handler`);
                          }
                      }, { _static: true });
    }

    installBackbeatErrorResponder(action, error, options) {
        this.setParam(`routes.${action}.handler`,
                      (req, url, query, res) => {
                          routesUtils.responseJSONBody(
                              error, null, res, this.log.newRequestLogger());
                          if (options && options.once) {
                              this.resetParam(`routes.${action}.handler`);
                          }
                      }, { _static: true });
    }

    // default handlers

    _getBucketReplication(req, url, query, res) {
        res.setHeader('content-type', 'application/xml');
        res.writeHead(200);
        res.end(['<?xml version="1.0" encoding="UTF-8"?>',
            '<ReplicationConfiguration ',
            'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
            '<Rule>',
            '<ID>myrule</ID>', '<Prefix/>',
            '<Status>',
            this.getParam('replicationEnabled') ? 'Enabled' : 'Disabled',
            '</Status>',
            '<Destination>',
            '<Bucket>',
            this.getParam('source.md.replicationInfo.destination'),
            '</Bucket>',
            '<StorageClass>sf</StorageClass>',
            '</Destination>',
            '</Rule>',
            '<Role>',
            this.getParam('source.md.replicationInfo.role'),
            '</Role>',
            '</ReplicationConfiguration>'].join(''));
    }

    _getAccountsCanonicalIds(req, url, query, res) {
        assert.strictEqual(Number(query.accountIds),
                           this.getParam('target.accountId'));

        res.writeHead(200);
        res.end(JSON.stringify([
            {
                accountId: query.accountId,
                canonicalId: this.getParam('target.canonicalId'),
            },
        ]));
    }

    _assumeRoleBackbeatSource(req, url, query, res) {
        assert.strictEqual(query.RoleArn,
                           this.getParam('source.role'));
        assert.strictEqual(query.RoleSessionName,
                           this.getParam('roleSessionName'));

        res.setHeader('content-type', 'application/json');
        res.writeHead(200);
        res.end(JSON.stringify({
            Credentials: {
                AccessKeyId: this.getParam('source.accessKey'),
                SecretAccessKey: this.getParam('source.secretKey'),
                SessionToken: 'dummySessionToken',
                Expiration: 1501108900014,
            },
            AssumedRoleUser: this.getParam('source.assumedRole'),
        }));
    }

    _assumeRoleBackbeatTarget(req, url, query, res) {
        assert.strictEqual(query.RoleArn,
                           this.getParam('target.role'));
        assert.strictEqual(query.RoleSessionName,
                           this.getParam('roleSessionName'));

        res.setHeader('content-type', 'application/json');
        res.writeHead(200);
        res.end(JSON.stringify({
            Credentials: {
                AccessKeyId: this.getParam('target.accessKey'),
                SecretAccessKey: this.getParam('target.secretKey'),
                SessionToken: 'dummySessionToken',
                Expiration: 1501108900014,
            },
            AssumedRoleUser: this.getParam('target.assumedRole'),
        }));
    }

    _getObject(req, url, query, res) {
        const partNumber = Number.parseInt(query.partNumber, 10);
        const resBody = this.getParam('partsContents')[partNumber - 1];
        assert.strictEqual(query.versionId,
                           this.getParam('versionIdEncoded'));

        res.setHeader('content-type', 'application/octet-stream');
        res.setHeader('content-length', resBody.length);
        res.writeHead(200);
        res.end(resBody);
    }

    _putData(req, url, query, res, reqBody) {
        const srcLocations = this.getParam('source.md.location');
        const destLocations = this.getParam('target.md.location');
        const md5 = req.headers['content-md5'];
        const { dataStoreETag } = srcLocations.find(
            location => location.dataStoreETag.includes(md5));
        const partNumber = Number(dataStoreETag.split(':')[0]);
        assert.strictEqual(
            reqBody, this.getParam('partsContents')[partNumber - 1]);

        res.setHeader('content-type', 'application/json');
        res.writeHead(200);
        res.end(JSON.stringify([destLocations[partNumber - 1]]));
        this.partsWritten[partNumber - 1] = true;
        this.hasPutTargetData =
            (this.partsWritten.filter(written => written === true).length
             === srcLocations.length);
    }

    _putMetadataTarget(req, url, query, res, reqBody) {
        if (this.getParam('nbParts') > 0) {
            if (this.getParam('source.md.replicationInfo.content')
                .includes('DATA')) {
                assert.strictEqual(this.hasPutTargetData, true);
            } else if (req.headers['x-scal-replication-content']
                       === 'METADATA') {
                assert.strictEqual(this.hasPutTargetData, false);
            } else {
                assert.strictEqual(this.hasPutTargetData, true);
            }
        }
        assert.strictEqual(this.hasPutTargetMd, false);

        const parsedMd = JSON.parse(reqBody);
        const replicatedContent =
                  this.getParam('source.md.replicationInfo.content');
        assert.deepStrictEqual(parsedMd.replicationInfo, {
            status: 'REPLICA',
            backends: [{
                site: 'sf',
                status: 'REPLICA',
                dataStoreVersionId: '',
            }, {
                site: 'replicationaws',
                status: 'PENDING',
                dataStoreVersionId: '',
            }],
            content: replicatedContent,
            destination: this.getParam('source.md.replicationInfo.destination'),
            storageClass: 'sf',
            role: this.getParam('source.md.replicationInfo.role'),
        });
        assert.strictEqual(parsedMd['owner-id'],
                           this.getParam('target.canonicalId'));

        if (req.headers['x-scal-replication-content'] === 'METADATA') {
            assert.deepStrictEqual(parsedMd.location, null);
        } else {
            assert.deepStrictEqual(parsedMd.location,
                                   this.getParam('target.md.location'));
        }

        res.writeHead(200);
        res.end();
        this.hasPutTargetMd = true;
    }

    _putMetadataSource(req, url, query, res, reqBody) {
        assert.strictEqual(this.hasPutTargetMd,
                           (this.expectedReplicationStatus === 'PROCESSING'));
        assert.notStrictEqual(this.onPutSourceMd, null);

        const parsedMd = JSON.parse(reqBody);
        assert.deepStrictEqual(parsedMd.replicationInfo, {
            status: this.expectedReplicationStatus,
            backends: [{
                site: 'sf',
                status: this.expectedReplicationStatus === 'FAILED' ? 'FAILED' :
                    'COMPLETED',
                dataStoreVersionId: '',
            }, {
                site: 'replicationaws',
                status: 'PENDING',
                dataStoreVersionId: '',
            }],
            content: this.getParam('source.md.replicationInfo.content'),
            destination: this.getParam('source.md.replicationInfo.destination'),
            storageClass: 'sf',
            role: this.getParam('source.md.replicationInfo.role'),
        });
        assert.strictEqual(parsedMd['owner-id'],
                           this.getParam('source.canonicalId'));

        res.writeHead(200);
        res.end();
        this.onPutSourceMd();
        this.onPutSourceMd = null;
    }

    _getMetadataSource(req, url, query, res) {
        assert(query.versionId);
        res.writeHead(200);
        res.end(JSON.stringify({
            Body: JSON.parse(this.getParam('kafkaEntry.value')).value,
        }));
    }
}

class MetricsMock {
    publishMetrics() {}
}

/* eslint-enable max-len */

describe('queue processor functional tests with mocking', () => {
    let queueProcessor;
    let replicationStatusProcessor;
    let httpServer;
    let s3mock;

    before(function before(done) {
        this.timeout(60000);
        const serverList =
                  constants.target.hosts.map(h => `${h.host}:${h.port}`);

        queueProcessor = new QueueProcessor(
            { hosts: 'localhost:9092' },
            { auth: { type: 'role',
                vault: { host: constants.source.vault,
                    port: 7777 } },
                s3: { host: constants.source.s3,
                    port: 7777 },
                transport: 'http',
            },
            { auth: { type: 'role' },
                bootstrapList: [{
                    site: 'sf', servers: serverList,
                }],
                transport: 'http' },
            { topic: 'backbeat-func-test-dummy-topic',
              replicationStatusTopic: 'backbeat-func-test-repstatus',
              queueProcessor: {
                  retryTimeoutS: 5,
                  groupId: 'backbeat-func-test-group-id',
              },
            }, new MetricsMock());
        queueProcessor.start({ disableConsumer: true });
        // create the replication status processor only when the queue
        // processor is ready, so that we ensure the replication
        // status topic is created, otherwise the consumer may be
        // stuck waiting for entries.
        queueProcessor.on('ready', () => {
            replicationStatusProcessor = new ReplicationStatusProcessor(
                { hosts: 'localhost:9092' },
                { auth: { type: 'role',
                          vault: { host: constants.source.vault,
                                   port: 7777 } },
                  s3: { host: constants.source.s3,
                        port: 7777 },
                  transport: 'http',
                },
                { replicationStatusTopic: 'backbeat-func-test-repstatus',
                  replicationStatusProcessor: {
                      retryTimeoutS: 5,
                      groupId: 'backbeat-func-test-group-id',
                  },
              });
            replicationStatusProcessor.start();
            replicationStatusProcessor.start({ bootstrap: true }, done);
        });

        s3mock = new S3Mock();
        httpServer = http.createServer(
            (req, res) => s3mock.onRequest(req, res));
        httpServer.listen(7777);
    });

    after(done => {
        httpServer.close();
        queueProcessor.stop(() => {
            replicationStatusProcessor.stop(done);
        });
    });

    afterEach(() => {
        s3mock.resetTest();
    });

    describe('success path tests', function successPath() {
        this.timeout(10000);

        [{ caption: 'object with single part',
            nbParts: 1 },
        { caption: 'object with multiple parts',
            nbParts: 2 },
        { caption: 'empty object',
            nbParts: 0 }].forEach(testCase => describe(testCase.caption, () => {
                before(() => {
                    s3mock.setParam('nbParts', testCase.nbParts);
                });
                it('should complete a replication end-to-end', done => {
                    async.parallel([
                        done => {
                            s3mock.onPutSourceMd = done;
                        },
                        done => queueProcessor.processKafkaEntry(
                            s3mock.getParam('kafkaEntry'), err => {
                                assert.ifError(err);
                                assert.strictEqual(s3mock.hasPutTargetData,
                                                   testCase.nbParts > 0);
                                assert(s3mock.hasPutTargetMd);
                                done();
                            }),
                    ], done);
                });
            }));

        it('should replicate metadata in metadata-only mode', done => {
            s3mock.setParam('nbParts', 2);
            s3mock.setParam('source.md.replicationInfo.content',
                            ['METADATA']);
            async.parallel([
                done => {
                    s3mock.onPutSourceMd = done;
                },
                done => queueProcessor.processKafkaEntry(
                    s3mock.getParam('kafkaEntry'), err => {
                        assert.ifError(err);
                        assert.strictEqual(s3mock.hasPutTargetData, false);
                        assert(s3mock.hasPutTargetMd);
                        done();
                    }),
            ], done);
        });

        it('should retry with full replication if metadata-only returns ' +
        'ObjNotFound', done => {
            s3mock.setParam('nbParts', 2);
            s3mock.setParam('source.md.replicationInfo.content',
                            ['METADATA']);
            s3mock.installBackbeatErrorResponder('target.putMetadata',
                                                 errors.ObjNotFound,
                                                 { once: true });
            async.parallel([
                done => {
                    s3mock.onPutSourceMd = done;
                },
                done => queueProcessor.processKafkaEntry(
                    s3mock.getParam('kafkaEntry'), err => {
                        assert.ifError(err);
                        assert.strictEqual(s3mock.hasPutTargetData, true);
                        assert(s3mock.hasPutTargetMd);
                        done();
                    }),
            ], done);
        });
    });

    describe('error paths', function errorPaths() {
        this.timeout(20000); // give more time to leave room for retry
                             // delays and timeout

        describe('source Vault errors', () => {
            ['assumeRoleBackbeat'].forEach(action => {
                [errors.AccessDenied, errors.NoSuchEntity].forEach(error => {
                    it(`should skip processing on ${error.code} ` +
                    `(${error.message}) from source Vault on ${action}`,
                    done => {
                        s3mock.installVaultErrorResponder(
                            `source.vault.${action}`, error);

                        queueProcessor.processKafkaEntry(
                            s3mock.getParam('kafkaEntry'), err => {
                                assert.ifError(err);
                                assert(!s3mock.hasPutTargetData);
                                assert(!s3mock.hasPutTargetMd);
                                done();
                            });
                    });
                });
            });
        });

        describe('source S3 errors', () => {
            [errors.AccessDenied, errors.ObjNotFound].forEach(error => {
                it(`should skip on ${error.code} (${error.message}) ` +
                'from source S3 on getObject', done => {
                    s3mock.installS3ErrorResponder('source.s3.getObject',
                                                   error);
                    queueProcessor.processKafkaEntry(
                        s3mock.getParam('kafkaEntry'), err => {
                            assert.ifError(err);
                            assert(!s3mock.hasPutTargetData);
                            assert(!s3mock.hasPutTargetMd);
                            done();
                        });
                });
            });

            it('should fail if replication is disabled in bucket replication ' +
            'configuration', done => {
                s3mock.setParam('replicationEnabled', false);
                s3mock.setExpectedReplicationStatus('FAILED');

                async.parallel([
                    done => {
                        s3mock.onPutSourceMd = done;
                    },
                    done => queueProcessor.processKafkaEntry(
                        s3mock.getParam('kafkaEntry'), err => {
                            assert.ifError(err);
                            assert(!s3mock.hasPutTargetData);
                            assert(!s3mock.hasPutTargetMd);
                            done();
                        }),
                ], done);
            });

            it('should fail if object misses dataStoreETag property', done => {
                s3mock.setParam(
                    'source.md.location',
                    buildLocations(s3mock.getParam('source.dataPartsKeys'),
                                   s3mock.getParam('partsContents'),
                                   { doNotIncludeETag: true }));
                s3mock.setExpectedReplicationStatus('FAILED');

                async.parallel([
                    done => {
                        s3mock.onPutSourceMd = done;
                    },
                    done => queueProcessor.processKafkaEntry(
                        s3mock.getParam('kafkaEntry'), err => {
                            assert.ifError(err);
                            assert(!s3mock.hasPutTargetData);
                            assert(!s3mock.hasPutTargetMd);
                            done();
                        }),
                ], done);
            });

            ['getBucketReplication', 'getObject'].forEach(action => {
                [errors.InternalError].forEach(error => {
                    it(`should retry on ${error.code} (${error.message}) ` +
                    `from source S3 on ${action}`, done => {
                        s3mock.installS3ErrorResponder(
                            `source.s3.${action}`, error, { once: true });

                        async.parallel([
                            done => {
                                s3mock.onPutSourceMd = done;
                            },
                            done => queueProcessor.processKafkaEntry(
                                s3mock.getParam('kafkaEntry'), err => {
                                    assert.ifError(err);
                                    assert(s3mock.hasPutTargetData);
                                    assert(s3mock.hasPutTargetMd);
                                    done();
                                }),
                        ], done);
                    });
                });
            });
        });

        describe('target Vault errors', () => {
            ['getAccountsCanonicalIds',
                'assumeRoleBackbeat'].forEach(action => {
                    [errors.AccessDenied, errors.NoSuchEntity].forEach(err => {
                        it(`should fail on ${err.code} (${err.message}) ` +
                        `from target Vault on ${action}`, done => {
                            s3mock.installVaultErrorResponder(
                             `target.${action}`, err);
                            s3mock.setExpectedReplicationStatus('FAILED');

                            async.parallel([
                                done => {
                                    s3mock.onPutSourceMd = done;
                                },
                                done => queueProcessor.processKafkaEntry(
                                    s3mock.getParam('kafkaEntry'), error => {
                                        assert.ifError(error);
                                        assert(!s3mock.hasPutTargetData);
                                        assert(!s3mock.hasPutTargetMd);
                                        done();
                                    }),
                            ], done);
                        });
                    });
                });

            ['getAccountsCanonicalIds',
            'assumeRoleBackbeat'].forEach(action => {
                [errors.InternalError].forEach(error => {
                    it(`should retry on ${error.code} (${error.message}) ` +
                    `from target Vault on ${action}`, done => {
                        s3mock.installVaultErrorResponder(
                            `target.${action}`, error, { once: true });

                        async.parallel([
                            done => {
                                s3mock.onPutSourceMd = done;
                            },
                            done => queueProcessor.processKafkaEntry(
                                s3mock.getParam('kafkaEntry'), err => {
                                    assert.ifError(err);
                                    assert(s3mock.hasPutTargetData);
                                    assert(s3mock.hasPutTargetMd);
                                    // should have retried on other host
                                    assert(s3mock.requestsPerHost['127.0.0.3']
                                           > 0);
                                    assert(s3mock.requestsPerHost['127.0.0.4']
                                           > 0);
                                    done();
                                }),
                        ], done);
                    });
                });
            });
        });

        describe('target S3 errors', () => {
            ['putData', 'putMetadata'].forEach(action => {
                [errors.AccessDenied].forEach(error => {
                    it(`should fail on ${error.code} (${error.message}) ` +
                    `from target S3 on ${action}`, done => {
                        s3mock.installS3ErrorResponder(`target.${action}`,
                                                       error);
                        s3mock.setExpectedReplicationStatus('FAILED');

                        async.parallel([
                            done => {
                                s3mock.onPutSourceMd = done;
                            },
                            done => queueProcessor.processKafkaEntry(
                                s3mock.getParam('kafkaEntry'), err => {
                                    assert.ifError(err);
                                    assert(!s3mock.hasPutTargetMd);
                                    done();
                                }),
                        ], done);
                    });
                });
            });

            ['putData', 'putMetadata'].forEach(action => {
                [errors.InternalError].forEach(error => {
                    it(`should retry on ${error.code} (${error.message}) ` +
                    `from target S3 on ${action}`, done => {
                        s3mock.installS3ErrorResponder(`target.${action}`,
                                                       error, { once: true });
                        async.parallel([
                            done => {
                                s3mock.onPutSourceMd = done;
                            },
                            done => queueProcessor.processKafkaEntry(
                                s3mock.getParam('kafkaEntry'), err => {
                                    assert.ifError(err);
                                    assert(s3mock.hasPutTargetData);
                                    assert(s3mock.hasPutTargetMd);
                                    // should have retried on other host
                                    assert(s3mock.requestsPerHost['127.0.0.3']
                                           > 0);
                                    assert(s3mock.requestsPerHost['127.0.0.4']
                                           > 0);
                                    done();
                                }),
                        ], done);
                    });
                });
            });
        });

        describe('retry behavior', () => {
            it('should give up retries after configured timeout (5s)',
            done => {
                s3mock.installS3ErrorResponder('source.s3.getObject',
                                               errors.InternalError);
                s3mock.setExpectedReplicationStatus('FAILED');

                async.parallel([
                    done => {
                        s3mock.onPutSourceMd = done;
                    },
                    done => queueProcessor.processKafkaEntry(
                        s3mock.getParam('kafkaEntry'), err => {
                            assert.ifError(err);
                            assert(!s3mock.hasPutTargetData);
                            assert(!s3mock.hasPutTargetMd);
                            done();
                        }),
                ], done);
            });
        });
    });
});
