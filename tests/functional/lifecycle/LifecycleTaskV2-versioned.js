const assert = require('assert');
const Logger = require('werelogs').Logger;
const { ObjectMD } = require('arsenal').models;

const { BackbeatMetadataProxyMock, expectNominalListingParams, KeyMock, S3Mock, TestKafkaEntry } = require('./utils');
const LifecycleTaskV2 = require('../../../extensions/lifecycle/tasks/LifecycleTaskV2');

const log = new Logger('LifecycleTaskV2:test');

const nonCurrentExpirationRule = [{
    NoncurrentVersionExpiration: { NoncurrentDays: 1 },
    ID: '123',
    Prefix: '',
    Status: 'Enabled',
}];

const bucketName = 'bucket1versioned';
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
const contentLength = 64;
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

describe('LifecycleTaskV2 with bucket versioned', () => {
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

        s3 = new S3Mock({ versionStatus: 'Enabled' });
    });

    afterEach(() => {
        kafkaEntries = [];
    });

    it('should not publish any entry if object is not eligible', done => {
        const nbRetries = 0;
        const contents = [
            keyMock.nonCurrent({ keyName: 'key1', versionId: 'versionid1', daysEarlier: 0 }),
        ];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'noncurrent');
            return done();
        });
    });

    it('should not publish any object entry if transition is already transitioned', done => {
        const nbRetries = 0;
        const transitionRule = [
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 1, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 1 });
        key.StorageClass = destinationLocation;
        key.DataStoreName = destinationLocation;
        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'noncurrent');
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
        const versionId = 'versionid1';
        const beforeDate = new Date().toISOString();
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 1 });

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        const bd = { ...bucketData, details: {
            keyMarker: 'key0',
            versionIdMarker: 'versionid0',
            listType: 'noncurrent',
            prefix,
            beforeDate,
        } };

        lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bd, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'noncurrent');
            // test parameters used to list lifecycle keys
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.KeyMarker, 'key0');
            assert.strictEqual(listLifecycleParams.VersionIdMarker, 'versionid0');
            assert.strictEqual(listLifecycleParams.Prefix, prefix);
            assert(!!listLifecycleParams.BeforeDate);

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            // test that the entry is valid and pushed to kafka topic
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, versionId });
            return done();
        });
    });

    it('should publish one object entry if object is eligible', done => {
        const nbRetries = 0;
        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 1 });

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'noncurrent');
            // test parameters used to list lifecycle keys
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            expectNominalListingParams(bucketName, listLifecycleParams);

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            // test that the entry is valid and pushed to kafka topic
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, versionId });
            return done();
        });
    });

    it('should publish one object entry if object is eligible with NoncurrentVersionTransitions rule', done => {
        const nbRetries = 0;
        const transitionRule = [
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 1, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 1 });
        const { ETag, LastModified } = key;

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'noncurrent');
            // test parameters used to list lifecycle keys
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            expectNominalListingParams(bucketName, listLifecycleParams);

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            // test that the entry is valid and pushed to kafka topic
            testKafkaEntry.expectObjectTransitionEntry(firstEntry, {
                keyName,
                versionId,
                lastModified: LastModified,
                eTag: ETag,
                contentLength,
                sourceLocation,
                destinationLocation,
            });
            return done();
        });
    });

    it('should publish one bucket entry to list orphan delete markers if Expiration rule is set', done => {
        const nbRetries = 0;
        const expirationRule = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 0 });

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
            testKafkaEntry.expectBucketEntry(firstEntry, {
                listType: 'orphan',
                hasBeforeDate: true,
                prefix: '',
            });
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
            // test parameters used to list lifecycle keys
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            expectNominalListingParams(bucketName, listLifecycleParams);

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

    it('should publish one object entry if object is eligible with ExpiredObjectDeleteMarker', done => {
        const nbRetries = 0;
        const transitionRule = [
            {
                Expiration: { ExpiredObjectDeleteMarker: true },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'deletemarker1';
        const versionId = 'versionid1';
        const key = keyMock.orphanDeleteMarker({ keyName, versionId, daysEarlier: 1 });

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse({ contents, isTruncated: false, markerInfo: {} });

        lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'orphan');
            // test parameters used to list lifecycle keys
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, '');
            assert(!listLifecycleParams.BeforeDate);

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];

            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, versionId });
            return done();
        });
    });

    it('should publish one bucket entry if listing is trucated', done => {
        const nbRetries = 0;
        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, daysEarlier: 0 });

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse(
            { contents, isTruncated: true, markerInfo: { keyMarker: keyName, versionIdMarker: versionId } }
        );

        lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'noncurrent');
            // test parameters used to list lifecycle keys
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            expectNominalListingParams(bucketName, listLifecycleParams);

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            // test that the entry is valid and pushed to kafka topic
            testKafkaEntry.expectBucketEntry(firstEntry, {
                hasBeforeDate: true,
                keyMarker: keyName,
                versionIdMarker: versionId,
                prefix: '',
            });
            return done();
        });
    });

    it('should publish one bucket and one object entry if object is elligible and listing is trucated', done => {
        const nbRetries = 0;
        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 1 });

        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse(
            { contents, isTruncated: true, markerInfo: { marker: keyName } }
        );

        lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 2);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'noncurrent');
            // test parameters used to list lifecycle keys
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            expectNominalListingParams(bucketName, listLifecycleParams);

            // test that the entry is valid and pushed to kafka topic
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectBucketEntry(firstEntry, {
                hasBeforeDate: true,
                marker: keyName,
                prefix: '',
            });

            const secondEntry = kafkaEntries[1];
            testKafkaEntry.expectObjectExpirationEntry(secondEntry, { keyName, versionId });
            return done();
        });
    });

    it('should publish one bucket entry if multiple rules', done => {
        const nbRetries = 0;
        const multipleRules = [
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                ID: '0',
                Prefix: 'pre1',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 2, StorageClass: destinationLocation }],
                ID: '1',
                Prefix: 'pre2',
                Status: 'Enabled',
            }
        ];
        const keyName = 'pre1-key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 0 });
        const contents = [key];
        backbeatMetadataProxy.setListLifecycleResponse(
            { contents, isTruncated: false, markerInfo: {} }
        );

        lifecycleTask.processBucketEntry(multipleRules, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.getListLifecycleType(), 'noncurrent');
            const listLifecycleParams = backbeatMetadataProxy.getListLifecycleParams();
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, 'pre1');
            assert(!!listLifecycleParams.BeforeDate);

            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            // test that the entry is valid and pushed to kafka topic
            testKafkaEntry.expectBucketEntry(firstEntry, {
                listType: 'noncurrent',
                hasBeforeDate: true,
                prefix: 'pre2',
            });
            return done();
        });
    });
});
