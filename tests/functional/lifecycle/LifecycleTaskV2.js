const assert = require('assert');
const async = require('async');
const { ObjectMD } = require('arsenal').models;

const Logger = require('werelogs').Logger;
const LifecycleTaskV2 = require('../../../extensions/lifecycle/tasks/LifecycleTaskV2');

const log = new Logger('LifecycleTaskV2:test');

const ONE_DAY_IN_SEC = 60 * 60 * 24 * 1000;

const simpleExpirationRule = [
    {
        Expiration: { Days: 1 },
        ID: '123',
        Prefix: '',
        Status: 'Enabled',
    }
];

const bucketName = 'bucket1';
const ownerId = 'f2a3ae88659516fbcad23cae38acc9fbdfcbcaf2e38c05d2e5e1bd1b9f930ff3';
const accountId = '345320934593';
const bucketData = {
    action: 'processObjects',
    target: {
      bucket: bucketName,
      owner: ownerId,
      accountId,
    },
    details: {}
};
const contentLength = 1000;
const sourceLocation = 'us-east-1';
const destinationLocation = 'us-east-2';

const bucketTopic = 'bucket-topic';
const objectTopic = 'object-topic';
const dataMoverTopic = 'backbeat-data-mover';

const kafkaClient = {
    getMetadata: (topicInfo, cb) => {
        return cb();
    }
};

const kafkaBacklogMetrics = {
    snapshotTopicOffsets: () => {}
};

const s3target = {
    getBucketVersioning: bucketName => {
        return {
            send: cb => {
                return cb(null, { Status: 'Disabled' });
            },
            on: () => {},
        };
    },
    listMultipartUploads: () => {
        return {
            send: cb => cb(null, { Uploads: [] }),
            on: () => {},
        };
    },
};

function makeObject({ keyName, daysEarlier }) {
    const currentDate = Date.now();
    const lastModified = (new Date(currentDate - (daysEarlier * ONE_DAY_IN_SEC))).toISOString();
    return {
        Key: keyName,
        LastModified: lastModified,
        ETag: 'd41d8cd98f00b204e9800998ecf8427e',
        Owner: {
            DisplayName: 'account1',
            ID: accountId,
        },
        Size: contentLength,
        StorageClass: 'STANDARD',
        TagSet: [],
        DataStoreName: sourceLocation,
        ListType: 'current'
    };
}


function expectObjectExpirationEntry(e, { keyName, lastModified }) {
    assert(e);
    assert.strictEqual(e.topicName, objectTopic);
    assert.strictEqual(e.entry.length, 1);

    const message = JSON.parse(e.entry[0].message);
    assert.strictEqual(message.action, 'deleteObject');

    const contextInfo = message.contextInfo;
    assert.strictEqual(contextInfo.origin, 'lifecycle');
    assert.strictEqual(contextInfo.ruleType, 'expiration');

    const target = message.target;
    assert.strictEqual(target.owner, ownerId);
    assert.strictEqual(target.accountId, accountId);
    assert.strictEqual(target.bucket, bucketName);
    assert.strictEqual(target.key, keyName);

    const details = message.details;
    assert.strictEqual(details.lastModified, lastModified);
}

function expectObjectTransitionEntry(e, { keyName, lastModified, eTag, contentLength }) {
    assert(e);
    assert.strictEqual(e.topicName, dataMoverTopic);
    assert.strictEqual(e.entry.length, 1);

    const message = JSON.parse(e.entry[0].message);
    assert.strictEqual(message.action, 'copyLocation');

    const contextInfo = message.contextInfo;
    assert.strictEqual(contextInfo.origin, 'lifecycle');
    assert.strictEqual(contextInfo.ruleType, 'transition');

    const target = message.target;
    assert.strictEqual(target.owner, ownerId);
    assert.strictEqual(target.accountId, accountId);
    assert.strictEqual(target.bucket, bucketName);
    assert.strictEqual(target.key, keyName);
    assert.strictEqual(target.eTag, eTag);
    assert.strictEqual(target.lastModified, lastModified);

    const metrics = message.metrics;
    assert.strictEqual(metrics.origin, 'lifecycle');
    assert.strictEqual(metrics.fromLocation, sourceLocation);
    assert.strictEqual(metrics.contentLength, contentLength);

    assert.strictEqual(message.toLocation, destinationLocation);
    assert.strictEqual(message.resultsTopic, objectTopic);
}

function expectBucketEntry(e, { hasBeforeDate, prefix, marker, keyMarker, versionIdMarker }) {
    assert(e);
    assert.strictEqual(e.topicName, bucketTopic);
    assert.strictEqual(e.entry.length, 1);

    const message = JSON.parse(e.entry[0].message);
    assert.strictEqual(message.action, 'processObjects');

    const target = message.target;
    assert.strictEqual(target.bucket, bucketName);
    assert.strictEqual(target.owner, ownerId);
    assert.strictEqual(target.accountId, accountId);

    const details = message.details;
    assert.strictEqual(!!details.beforeDate, hasBeforeDate);
    assert.strictEqual(details.prefix, prefix);
    assert.strictEqual(details.marker, marker);
    assert.strictEqual(details.keyMarker, keyMarker);
    assert.strictEqual(details.versionIdMarker, versionIdMarker);
}

class BackbeatMetadataProxyMock {
    constructor() {
        this.mdObj = null;
        this.receivedMd = null;
        this.contents = [];
        this.isTruncated = false;
        this.markerInfo = null;
    }

    setMdObj(mdObj) {
        this.mdObj = mdObj;
    }

    setListLifecycleResponse({ contents, isTruncated, markerInfo }) {
        this.contents = contents;
        this.isTruncated = isTruncated;
        this.markerInfo = markerInfo;
    }

    getReceivedMd() {
        return this.receivedMd;
    }

    listLifecycle(listType, params, log, cb) {
        return cb(null, this.contents, this.isTruncated, this.markerInfo);
    }

    getMetadata(params, log, cb) {
        console.log('this.mdObj.getSerialized()!!!', this.mdObj.getSerialized());
        return cb(null, { Body: this.mdObj.getSerialized() });
    }

    putMetadata(params, log, cb) {
        this.receivedMd = JSON.parse(params.mdBlob);
        this.mdObj = ObjectMD.createFromBlob(params.mdBlob).result;
        return cb();
    }
}

describe('LifecycleTaskV2', () => {
    let kafkaEntries = [];
    let lifecycleTask;
    let objMd;
    let backbeatMetadataProxy;

    before(() => {
        const producer = {
            sendToTopic: (topicName, entry, cb) => {
                kafkaEntries.push({ topicName, entry });
                cb(null, [{ partition: 1 }]);
            },
            getKafkaProducer: () => kafkaClient,
        };

        const lp = {
            getStateVars: () => {
                return {
                    producer,
                    bucketTasksTopic: bucketTopic,
                    objectTasksTopic: objectTopic,
                    kafkaBacklogMetrics,
                    log,
                };
            },
        };
        lifecycleTask = new LifecycleTaskV2(lp);
    });

    beforeEach(() => {
        backbeatMetadataProxy = new BackbeatMetadataProxyMock();
        objMd = new ObjectMD();
        objMd.setDataStoreName(sourceLocation);
        objMd.setContentLength(contentLength);
        backbeatMetadataProxy.setMdObj(objMd);
    });

    afterEach(() => {
        kafkaEntries = [];
    });

    it('should not publish any entry if bucket is empty', done => {
        const contents = [
            makeObject({ keyName: 'key1', daysEarlier: 0 }),
        ];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(simpleExpirationRule, bucketData, s3target,
        backbeatMetadataProxy, 0, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            return done();
        });
    });

    it('should not publish any entry if rule disabled', done => {
        const disabledRule = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: '',
                Status: 'Disabled',
            }
        ];

        const contents = [
            makeObject({ keyName: 'key1', daysEarlier: 1 }),
        ];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(disabledRule, bucketData, s3target,
            backbeatMetadataProxy, 0, err => {
                assert.ifError(err);
                assert.strictEqual(kafkaEntries.length, 0);
                return done();
            });
    });

    it('should not publish any entry if object is not eligible', done => {
        const contents = [
            makeObject({ keyName: 'key1', daysEarlier: 0 }),
        ];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(simpleExpirationRule, bucketData, s3target,
        backbeatMetadataProxy, 0, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            return done();
        });
    });

    it('should publish one object entry if object is eligible', done => {
        const keyName = 'key1';
        const key = makeObject({ keyName, daysEarlier: 1 });
        const { LastModified } = key;

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(simpleExpirationRule, bucketData, s3target,
        backbeatMetadataProxy, 0, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            expectObjectExpirationEntry(firstEntry, { keyName, lastModified: LastModified });
            return done();
        });
    });

    it('should publish one object entry if object is eligible with Transitions rule', done => {
        const transitionRule = [
            {
                Transitions: [{ Days: 1, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const key = makeObject({ keyName, daysEarlier: 1 });
        const { ETag, LastModified } = key;

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(transitionRule, bucketData, s3target,
        backbeatMetadataProxy, 0, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            expectObjectTransitionEntry(firstEntry, { keyName, lastModified: LastModified, eTag: ETag, contentLength });
            return done();
        });
    });

    it('should publish one bucket entry if listing is trucated', done => {
        const keyName = 'key1';
        const key = makeObject({ keyName, daysEarlier: 0 });

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse(
            { contents, isTruncated: true, markerInfo: { marker: keyName } }
        );

        lifecycleTask.processBucketEntry(simpleExpirationRule, bucketData, s3target,
        backbeatMetadataProxy, 0, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            expectBucketEntry(firstEntry, {
                hasBeforeDate: true,
                marker: keyName,
                prefix: '',
            });
            return done();
        });
    });

    it('should publish one bucket and one object entry if object is elligible and listing is trucated', done => {
        const keyName = 'key1';
        const key = makeObject({ keyName, daysEarlier: 1 });
        const { LastModified } = key;

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse(
            { contents, isTruncated: true, markerInfo: { marker: keyName } }
        );

        lifecycleTask.processBucketEntry(simpleExpirationRule, bucketData, s3target,
        backbeatMetadataProxy, 0, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 2);

            const firstEntry = kafkaEntries[0];
            expectBucketEntry(firstEntry, {
                hasBeforeDate: true,
                marker: keyName,
                prefix: '',
            });

            const secondEntry = kafkaEntries[1];
            expectObjectExpirationEntry(secondEntry, { keyName, lastModified: LastModified });
            return done();
        });
    });

    it('should publish one bucket entry if multiple rules', done => {
        const multipleRules = [
            {
                Expiration: { Days: 1 },
                ID: '0',
                Prefix: 'pre1',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 2, StorageClass: destinationLocation }],
                ID: '1',
                Prefix: 'pre2',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const key = makeObject({ keyName, daysEarlier: 1 });
        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse(
            { contents, isTruncated: false, markerInfo: {} }
        );

        lifecycleTask.processBucketEntry(multipleRules, bucketData, s3target,
        backbeatMetadataProxy, 0, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            expectBucketEntry(firstEntry, {
                hasBeforeDate: true,
                prefix: 'pre2',
            });
            return done();
        });
    });
});
