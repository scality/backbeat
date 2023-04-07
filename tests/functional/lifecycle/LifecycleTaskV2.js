const assert = require('assert');
const Logger = require('werelogs').Logger;
const { ObjectMD } = require('arsenal').models;

const { S3ClientMock } = require('../../utils/S3ClientMock');
const { BackbeatMetadataProxyMock, expectNominalListingParams, KeyMock, TestKafkaEntry } = require('./utils');
const LifecycleTaskV2 = require('../../../extensions/lifecycle/tasks/LifecycleTaskV2');

const log = new Logger('LifecycleTaskV2:test');
const ONE_DAY_IN_SEC = 60 * 60 * 24 * 1000;

const expirationRule = [{
    Expiration: { Days: 2 },
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
                pausedLocations: new Set(),
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
        backbeatMetadataProxy.objectMetadata = objMd;

        s3 = new S3ClientMock({});
        s3.stubMethod('getBucketVersioning', { Status: 'Disabled' });
    });

    afterEach(() => {
        kafkaEntries = [];
    });

    it('should not publish any entry if bucket is empty', done => {
        const contents = [];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(expirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            expectNominalListingParams(bucketName, listLifecycleParams);
            return done();
        });
    });

    it('should not publish any entry if rule disabled', done => {
        const disabledRule = [
            {
                Expiration: { Days: 2 },
                ID: '123',
                Prefix: '',
                Status: 'Disabled',
            }
        ];

        const contents = [
            keyMock.current({ keyName: 'key1', daysEarlier: 1 }),
        ];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(disabledRule, bucketData, s3,
            backbeatMetadataProxy, nbRetries, err => {
                assert.ifError(err);
                assert.strictEqual(kafkaEntries.length, 0);
                // test that listing has not been triggered
                assert.strictEqual(backbeatMetadataProxy.listLifecycleType, null);
                assert.strictEqual(backbeatMetadataProxy.listLifecycleParams, null);
                return done();
            });
    });

    it('should not publish any entry if object is not eligible', done => {
        const contents = [
            keyMock.current({ keyName: 'key1', daysEarlier: 0 }),
        ];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(expirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            expectNominalListingParams(bucketName, listLifecycleParams);
            return done();
        });
    });

    it('should not publish any entry if detail section is comming from the old lifecycle task', done => {
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 1 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const bd = { ...bucketData, details: {
            marker: 'key0',
        } };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(expirationRule, bd, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that listing has not been triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, null);
            assert.strictEqual(backbeatMetadataProxy.listLifecycleParams, null);
            return done();
        });
    });

    it('should not publish any object entry if detail section is invalid', done => {
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 1 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const bd = { ...bucketData, details: {
            listType: 'invalid',
        } };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(expirationRule, bd, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that listing has not been triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, null);
            assert.strictEqual(backbeatMetadataProxy.listLifecycleParams, null);
            return done();
        });
    });

    it('should not publish any object entry if transition is already transitioned', done => {
        const transitionRule = [
            {
                Transitions: [{ Days: 2, StorageClass: destinationLocation }],
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
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, '');
            assert.strictEqual(listLifecycleParams.ExcludedDataStoreName, destinationLocation);
            assert(!!listLifecycleParams.BeforeDate);
            return done();
        });
    });

    it('should publish one object with the bucketData details set', done => {
        const prefix = 'pre1';
        const keyName = `${prefix}key1`;
        const key = keyMock.current({ keyName, daysEarlier: 1 });
        const { LastModified } = key;
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const beforeDate = new Date().toISOString();
        const bd = { ...bucketData, details: {
            marker: 'key0',
            listType: 'current',
            prefix,
            beforeDate,
            storageClass: destinationLocation,
        } };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(expirationRule, bd, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, prefix);
            assert.strictEqual(listLifecycleParams.BeforeDate, beforeDate);
            assert.strictEqual(listLifecycleParams.Marker, 'key0');
            assert.strictEqual(listLifecycleParams.ExcludedDataStoreName, destinationLocation);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, lastModified: LastModified });
            return done();
        });
    });

    it('should publish one object entry if object is eligible', done => {
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 1 });
        const { LastModified } = key;
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(expirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            expectNominalListingParams(bucketName, listLifecycleParams);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, lastModified: LastModified });
            return done();
        });
    });

    it('should publish one object entry if tags and prefix match', done => {
        const prefix = 'pre1';
        const keyName = `${prefix}key1`;
        const tagSet = [
            { Key: 'key', Value: 'val' },
            { Key: 'key2', Value: 'val2' },
        ];
        const key = keyMock.current({ keyName, daysEarlier: 1, tagSet });
        const { LastModified } = key;

        const ruleWithTags = [{
            Expiration: { Days: 2 },
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
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(ruleWithTags, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, prefix);
            assert(!!listLifecycleParams.BeforeDate);
            assert.strictEqual(listLifecycleParams.Prefix, prefix);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, lastModified: LastModified });
            return done();
        });
    });

    it('should not publish one object entry if tags do not match', done => {
        const keyName = 'key1';
        const tagSet = [
            { Key: 'key', Value: 'val' },
            { Key: 'key2', Value: 'val2' },
        ];
        const key = keyMock.current({ keyName, daysEarlier: 1, tagSet });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const ruleWithTags = [{
            Expiration: { Days: 2 },
            ID: '123',
            Status: 'Enabled',
            Filter: {
                And: {
                    Tags: [...tagSet, { Key: 'key3', Value: 'val3' }],
                },
            },
        }];
        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(ruleWithTags, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);

            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            expectNominalListingParams(bucketName, listLifecycleParams);
            return done();
        });
    });

    it('should publish one object entry if object is eligible with Transitions rule', done => {
        const transitionRule = [
            {
                Transitions: [{ Days: 2, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];

        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 1 });
        const { ETag, LastModified } = key;
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, '');
            assert.strictEqual(listLifecycleParams.ExcludedDataStoreName, destinationLocation);
            assert(!!listLifecycleParams.BeforeDate);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
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

    it('should not publish any object entry if object is not eligible with Transitions rule', done => {
        const transitionRule = [
            {
                Transitions: [{ Days: 2, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];

        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 0 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, '');
            assert.strictEqual(listLifecycleParams.ExcludedDataStoreName, destinationLocation);
            assert(!!listLifecycleParams.BeforeDate);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 0);
            return done();
        });
    });

    it('should publish one bucket entry if listing is trucated', done => {
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 0 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse =
            { contents, isTruncated: true, markerInfo: { marker: keyName } };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(expirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            expectNominalListingParams(bucketName, listLifecycleParams);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectBucketEntry(firstEntry, {
                hasBeforeDate: true,
                marker: keyName,
                prefix: '',
                listType: 'current',
            });
            return done();
        });
    });

    it('should publish one bucket entry if listing keys to be transitioned is trucated', done => {
        const transitionRule = [
            {
                Transitions: [{ Days: 2, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 0 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse =
            { contents, isTruncated: true, markerInfo: { marker: keyName } };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, '');
            assert.strictEqual(listLifecycleParams.ExcludedDataStoreName, destinationLocation);
            assert(!!listLifecycleParams.BeforeDate);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectBucketEntry(firstEntry, {
                hasBeforeDate: true,
                marker: keyName,
                prefix: '',
                listType: 'current',
                storageClass: destinationLocation,
            });
            return done();
        });
    });

    it('should not publish bucket entry if listing is trucated but is retried', done => {
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 0 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse =
            { contents, isTruncated: true, markerInfo: { marker: keyName } };

        const nbRetries = 1;
        return lifecycleTask.processBucketEntry(expirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            expectNominalListingParams(bucketName, listLifecycleParams);
            return done();
        });
    });

    it('should publish one bucket and one object entry if object is elligible and listing is trucated', done => {
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 1 });
        const { LastModified } = key;
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse =
            { contents, isTruncated: true, markerInfo: { marker: keyName } };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(expirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 2);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            expectNominalListingParams(bucketName, listLifecycleParams);

            // test that the entry is valid and pushed to kafka topic
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectBucketEntry(firstEntry, {
                hasBeforeDate: true,
                marker: keyName,
                prefix: '',
                listType: 'current',
            });

            const secondEntry = kafkaEntries[1];
            testKafkaEntry.expectObjectExpirationEntry(secondEntry, { keyName, lastModified: LastModified });
            return done();
        });
    });

    it('should publish one bucket entry if multiple rules', done => {
        const multipleRules = [
            {
                Expiration: { Days: 2 },
                ID: '0',
                Prefix: 'pre1',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 3, StorageClass: destinationLocation }],
                ID: '1',
                Prefix: 'pre2',
                Status: 'Enabled',
            }
        ];

        const keyName = 'pre1-key1';
        const key = keyMock.current({ keyName, daysEarlier: 0 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(multipleRules, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, 'pre1');
            assert(!!listLifecycleParams.BeforeDate);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectBucketEntry(firstEntry, {
                listType: 'current',
                hasBeforeDate: true,
                prefix: 'pre2',
                storageClass: destinationLocation,
            });
            return done();
        });
    });

    it('should not publish a bucket entry if multiple rules but listing is retried', done => {
        const multipleRules = [
            {
                Expiration: { Days: 2 },
                ID: '0',
                Prefix: 'pre1',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 3, StorageClass: destinationLocation }],
                ID: '1',
                Prefix: 'pre2',
                Status: 'Enabled',
            }
        ];

        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 1 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 1;
        return lifecycleTask.processBucketEntry(multipleRules, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, 'pre1');
            assert(!!listLifecycleParams.BeforeDate);
            return done();
        });
    });

    it('should publish one mpu entry', done => {
        const mpuRule = [
            {
                AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 },
                ID: '0',
                Status: 'Enabled',
            },
        ];

        const keyName = 'mpu0';
        const uploadId = '4808badd7b1043df8289e06db07009b3';
        const currentDate = Date.now();
        const initiated = (new Date(currentDate - (1 * ONE_DAY_IN_SEC))).toISOString();
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
        s3.stubMethod('listMultipartUploads', mpuResponse);

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(mpuRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that listing has not been triggered.
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, null);
            assert.strictEqual(backbeatMetadataProxy.listLifecycleParams, null);

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            // test that the entry is valid and pushed to kafka topic
            testKafkaEntry.expectMPUExpirationEntry(firstEntry, { keyName, uploadId });
            return done();
        });
    });

    it('should publish one bucket entry and one mpu entry if MPU listing is truncated', done => {
        const mpuRule = [
            {
                AbortIncompleteMultipartUpload: { DaysAfterInitiation: 1 },
                ID: '0',
                Status: 'Enabled',
            },
        ];

        const keyName = 'mpu0';
        const uploadId = '4808badd7b1043df8289e06db07009b3';
        const uploadIdMarker = 'marker0';
        const currentDate = Date.now();
        const initiated = (new Date(currentDate - (1 * ONE_DAY_IN_SEC))).toISOString();
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

        s3.stubMethod('listMultipartUploads', mpuResponse);

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(mpuRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that listing has not been triggered.
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, null);
            assert.strictEqual(backbeatMetadataProxy.listLifecycleParams, null);

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
