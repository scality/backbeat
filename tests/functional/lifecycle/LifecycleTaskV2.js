const assert = require('assert');
const Logger = require('werelogs').Logger;
const { ObjectMD } = require('arsenal').models;

const { BackbeatMetadataProxyMock, expectNominalListingParams, KeyMock, S3Mock, TestKafkaEntry } = require('./utils');
const LifecycleTaskV2 = require('../../../extensions/lifecycle/tasks/LifecycleTaskV2');

const log = new Logger('LifecycleTaskV2:test');
const ONE_DAY_IN_SEC = 60 * 60 * 24 * 1000;

const expirationRule = [{
    Expiration: { Days: 1 },
    ID: '123',
    Prefix: '',
    Status: 'Enabled',
}];

const bucketName = 'bucket1';
const accountName = 'account1';
const ownerId = 'f2a3ae88659516fbcad23cae38acc9fbdfcbcaf2e38c05d2e5e1bd1b9f930ff3';
const owner = {
    DisplayName: accountName,
    ID: ownerId,
};
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
const testKafkaEntry = new TestKafkaEntry({
    objectTopic,
    bucketTopic,
    dataMoverTopic,
    ownerId,
    accountId,
    bucketName,
});

const keyMock = new KeyMock({ owner, sourceLocation });

describe('LifecycleTaskV2 with bucket non-versioned', () => {
    let kafkaEntries = [];
    let lifecycleTask;
    let objMd;
    let backbeatMetadataProxy;
    let s3;

    before(() => {
        const producer = {
            sendToTopic: (topicName, entry, cb) => {
                kafkaEntries.push({ topicName, entry });
                cb(null, [{ partition: 1 }]);
            },
            getKafkaProducer: () => {},
        };

        const lp = {
            getStateVars: () => ({
                producer,
                bucketTasksTopic: bucketTopic,
                objectTasksTopic: objectTopic,
                kafkaBacklogMetrics: { snapshotTopicOffsets: () => {} },
                log,
            }),
        };
        lifecycleTask = new LifecycleTaskV2(lp);
    });

    beforeEach(() => {
        backbeatMetadataProxy = new BackbeatMetadataProxyMock();
        objMd = new ObjectMD();
        objMd.setDataStoreName(sourceLocation);
        objMd.setContentLength(contentLength);
        backbeatMetadataProxy.setMdObj(objMd);

        s3 = new S3Mock({ versionStatus: 'Disabled' });
    });

    afterEach(() => {
        kafkaEntries = [];
    });

    it('should not publish any entry if bucket is empty', done => {
        const nbRetries = 0;
        const contents = [
            keyMock.current({ keyName: 'key1', daysEarlier: 0 }),
        ];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(expirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'current');
            // test parameters used to list lifecycle keys
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            expectNominalListingParams(bucketName, listLifecycleParams);
            return done();
        });
    });

    it('should not publish any entry if rule disabled', done => {
        const nbRetries = 0;
        const disabledRule = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: '',
                Status: 'Disabled',
            }
        ];

        const contents = [
            keyMock.current({ keyName: 'key1', daysEarlier: 1 }),
        ];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(disabledRule, bucketData, s3,
            backbeatMetadataProxy, nbRetries, err => {
                assert.ifError(err);
                assert.strictEqual(kafkaEntries.length, 0);
                // test that listing has not been triggered
                assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), null);
                assert.strictEqual(backbeatMetadataProxy.getListLifecycleParams(), null);
                return done();
            });
    });

    it('should not publish any entry if object is not eligible', done => {
        const nbRetries = 0;
        const contents = [
            keyMock.current({ keyName: 'key1', daysEarlier: 0 }),
        ];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(expirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'current');
            // test parameters used to list lifecycle keys
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            expectNominalListingParams(bucketName, listLifecycleParams);
            return done();
        });
    });

    it('should not publish any entry if detail section is comming from the old lifecycle task', done => {
        const nbRetries = 0;
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 1 });

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        const bd = { ...bucketData, details: {
            marker: 'key0',
        } };

        lifecycleTask.processBucketEntry(expirationRule, bd, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that listing has not been triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), null);
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleParams(), null);
            return done();
        });
    });

    it('should not publish any object entry if detail section is invalid', done => {
        const nbRetries = 0;
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 1 });

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        const bd = { ...bucketData, details: {
            listType: 'invalid',
        } };

        lifecycleTask.processBucketEntry(expirationRule, bd, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that listing has not been triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), null);
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleParams(), null);
            return done();
        });
    });

    it('should not publish any object entry if transition is already transitioned', done => {
        const nbRetries = 0;
        const transitionRule = [
            {
                Transitions: [{ Days: 1, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 1 });
        key.StorageClass = destinationLocation;
        key.DataStoreName = destinationLocation;

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'current');
            // test parameters used to list lifecycle keys
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            expectNominalListingParams(bucketName, listLifecycleParams);
            return done();
        });
    });

    it('should publish one object with the bucketData details set', done => {
        const nbRetries = 0;
        const prefix = 'pre1';
        const keyName = `${prefix}key1`;
        const beforeDate = new Date().toISOString();
        const key = keyMock.current({ keyName, daysEarlier: 1 });
        const { LastModified } = key;

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        const bd = { ...bucketData, details: {
            marker: 'key0',
            listType: 'current',
            prefix,
            beforeDate,
        } };

        lifecycleTask.processBucketEntry(expirationRule, bd, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'current');
            // test parameters used to list lifecycle keys
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, prefix);
            assert.strictEqual(listLifecycleParams.BeforeDate, beforeDate);
            assert.strictEqual(listLifecycleParams.Marker, 'key0');

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            // test that the entry is valid and pushed to kafka topic
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, lastModified: LastModified });
            return done();
        });
    });

    it('should publish one object entry if object is eligible', done => {
        const nbRetries = 0;
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 1 });
        const { LastModified } = key;

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(expirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'current');
            // test parameters used to list lifecycle keys
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            expectNominalListingParams(bucketName, listLifecycleParams);

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            // test that the entry is valid and pushed to kafka topic
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, lastModified: LastModified });
            return done();
        });
    });

    it('should publish one object entry if tags and prefix match', done => {
        const nbRetries = 0;
        const prefix = 'pre1';
        const keyName = `${prefix}key1`;
        const tagSet = [
            { Key: 'key', Value: 'val' },
            { Key: 'key2', Value: 'val2' },
        ];
        const key = keyMock.current({ keyName, daysEarlier: 1, tagSet });
        const { LastModified } = key;

        const ruleWithTags = [{
            Expiration: { Days: 1 },
            ID: '123',
            Status: 'Enabled',
            Filter: {
                And: {
                    Prefix: prefix,
                    Tags: tagSet,
                },
            },
        }];

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(ruleWithTags, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'current');
            // test parameters used to list lifecycle keys
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, prefix);
            assert(!!listLifecycleParams.BeforeDate);
            assert.strictEqual(listLifecycleParams.Prefix, prefix);

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            // test that the entry is valid and pushed to kafka topic
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, lastModified: LastModified });
            return done();
        });
    });

    it('should not publish one object entry if tags do not match', done => {
        const nbRetries = 0;
        const keyName = 'key1';
        const tagSet = [
            { Key: 'key', Value: 'val' },
            { Key: 'key2', Value: 'val2' },
        ];
        const key = keyMock.current({ keyName, daysEarlier: 1, tagSet });

        const ruleWithTags = [{
            Expiration: { Days: 1 },
            ID: '123',
            Status: 'Enabled',
            Filter: {
                And: {
                    Tags: [...tagSet, { Key: 'key3', Value: 'val3' }],
                },
            },
        }];

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(ruleWithTags, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'current');
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            expectNominalListingParams(bucketName, listLifecycleParams);
            return done();
        });
    });

    it('should publish one object entry if object is eligible with Transitions rule', done => {
        const nbRetries = 0;
        const transitionRule = [
            {
                Transitions: [{ Days: 1, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 1 });
        const { ETag, LastModified } = key;

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'current');

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            // test that the entry is valid and pushed to kafka topic
            testKafkaEntry.expectObjectTransitionEntry(firstEntry, {
                keyName,
                lastModified: LastModified,
                eTag: ETag,
                contentLength,
                sourceLocation,
                destinationLocation,
            });
            return done();
        });
    });

    it('should publish one bucket entry if listing is trucated', done => {
        const nbRetries = 0;
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 0 });

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse(
            { contents, isTruncated: true, markerInfo: { marker: keyName } }
        );

        lifecycleTask.processBucketEntry(expirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'current');

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            // test that the entry is valid and pushed to kafka topic
            testKafkaEntry.expectBucketEntry(firstEntry, {
                hasBeforeDate: true,
                marker: keyName,
                prefix: '',
            });
            return done();
        });
    });

    it('should not publish bucket entry if listing is trucated but is retried', done => {
        const nbRetries = 1;
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 0 });

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse(
            { contents, isTruncated: true, markerInfo: { marker: keyName } }
        );

        lifecycleTask.processBucketEntry(expirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'current');
            return done();
        });
    });

    it('should publish one bucket and one object entry if object is elligible and listing is trucated', done => {
        const nbRetries = 0;
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 1 });
        const { LastModified } = key;

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse(
            { contents, isTruncated: true, markerInfo: { marker: keyName } }
        );

        lifecycleTask.processBucketEntry(expirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 2);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'current');

            // test that the entry is valid and pushed to kafka topic
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectBucketEntry(firstEntry, {
                hasBeforeDate: true,
                marker: keyName,
                prefix: '',
            });

            const secondEntry = kafkaEntries[1];
            testKafkaEntry.expectObjectExpirationEntry(secondEntry, { keyName, lastModified: LastModified });
            return done();
        });
    });

    it('should publish one bucket entry if multiple rules', done => {
        const nbRetries = 0;
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
        const keyName = 'pre1-key1';
        const key = keyMock.current({ keyName, daysEarlier: 0 });
        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse(
            { contents, isTruncated: false, markerInfo: {} }
        );

        lifecycleTask.processBucketEntry(multipleRules, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'current');

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            // test that the entry is valid and pushed to kafka topic
            testKafkaEntry.expectBucketEntry(firstEntry, {
                listType: 'current',
                hasBeforeDate: true,
                prefix: 'pre2',
            });
            return done();
        });
    });

    it('should not publish a bucket entry if multiple rules but listing is retried', done => {
        const nbRetries = 1;
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
        const key = keyMock.current({ keyName, daysEarlier: 1 });
        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse(
            { contents, isTruncated: false, markerInfo: {} }
        );

        lifecycleTask.processBucketEntry(multipleRules, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'current');
            return done();
        });
    });

    it('should publish one mpu entry', done => {
        const nbRetries = 0;
        const mpuRule = [
            {
                AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 },
                ID: '0',
                Status: 'Enabled',
            },
        ];

        const currentDate = Date.now();
        const initiated = (new Date(currentDate - (1 * ONE_DAY_IN_SEC))).toISOString();

        const keyName = 'mpu0';
        const uploadId = '4808badd7b1043df8289e06db07009b3';
        const mpuResponse = {
            Bucket: bucketName,
            MaxUploads: 1000,
            IsTruncated: false,
            Uploads: [
                {
                    UploadId: uploadId,
                    Key: keyName,
                    Initiated: initiated,
                    StorageClass: 'STANDARD',
                    Owner: owner,
                    Initiator: owner,
                }
            ],
        };

        s3.setListMPUResponse(mpuResponse);

        lifecycleTask.processBucketEntry(mpuRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that listing has not been triggered.
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), null);
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleParams(), null);

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            // test that the entry is valid and pushed to kafka topic
            testKafkaEntry.expectMPUExpirationEntry(firstEntry, { keyName, uploadId });
            return done();
        });
    });

    it('should publish one bucket entry and one mpu entry if MPU listing is truncated', done => {
        const nbRetries = 0;
        const mpuRule = [
            {
                AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 },
                ID: '0',
                Status: 'Enabled',
            },
        ];

        const currentDate = Date.now();
        const initiated = (new Date(currentDate - (1 * ONE_DAY_IN_SEC))).toISOString();

        const keyName = 'mpu0';
        const uploadId = '4808badd7b1043df8289e06db07009b3';
        const uploadIdMarker = 'marker0';
        const mpuResponse = {
            Bucket: bucketName,
            NextKeyMarker: keyName,
            NextUploadIdMarker: uploadIdMarker,
            MaxUploads: 1000,
            IsTruncated: true,
            Uploads: [
                {
                    UploadId: uploadId,
                    Key: keyName,
                    Initiated: initiated,
                    StorageClass: 'STANDARD',
                    Owner: owner,
                    Initiator: owner,
                }
            ],
        };

        s3.setListMPUResponse(mpuResponse);

        lifecycleTask.processBucketEntry(mpuRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that listing has not been triggered.
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), null);
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleParams(), null);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 2);
            const firstEntry = kafkaEntries[0];
            const secondEntry = kafkaEntries[1];
            testKafkaEntry.expectBucketEntry(firstEntry, {
                hasBeforeDate: false,
                keyMarker: keyName,
                uploadIdMarker,
            });
            testKafkaEntry.expectMPUExpirationEntry(secondEntry, { keyName, uploadId });
            return done();
        });
    });
});
