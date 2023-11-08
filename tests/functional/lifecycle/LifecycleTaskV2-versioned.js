const assert = require('assert');
const Logger = require('werelogs').Logger;
const { ObjectMD } = require('arsenal').models;
const { S3ClientMock } = require('../../utils/S3ClientMock');
const { timeOptions } = require('./configObjects');

const { BackbeatMetadataProxyMock, expectNominalListingParams, KeyMock, TestKafkaEntry } = require('./utils');
const LifecycleTaskV2 = require('../../../extensions/lifecycle/tasks/LifecycleTaskV2');

const log = new Logger('LifecycleTaskV2:test:versioned');

const nonCurrentExpirationRule = [{
    NoncurrentVersionExpiration: { NoncurrentDays: 2 },
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

    beforeAll(() => {
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
                lcOptions: timeOptions,
                ncvHeap: new Map(),
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
        s3.stubMethod('getBucketVersioning', { Status: 'Enabled' });
    });

    afterEach(() => {
        kafkaEntries = [];
    });

    it('should not publish any entry if object is not eligible', done => {
        const contents = [
            keyMock.nonCurrent({ keyName: 'key1', versionId: 'versionid1', daysEarlier: 1 }),
        ];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the non-current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');
            return done();
        });
    });

    it('should not publish any object entry if transition is already transitioned', done => {
        const transitionRule = [
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 2, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 2 });
        key.StorageClass = destinationLocation;
        key.DataStoreName = destinationLocation;
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 0);
            // test that the non-current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, '');
            assert(!!listLifecycleParams.BeforeDate);
            assert.strictEqual(listLifecycleParams.ExcludedDataStoreName, destinationLocation);
            return done();
        });
    });

    it('should not publish any object if the bucketData details set to an outdated listing', done => {
        // When lifecycle rules get updated, listing might return objects that are not eligible anymore.
        // For example, here, the kafka message tells the Bucket Processor to list current message
        // whereas the lifecycle rules have been updated to expire non-current object.
        // The current objects should not be expired anymore.
        const prefix = 'pre1';
        const keyName = `${prefix}key1`;
        const key = keyMock.orphanDeleteMarker({ keyName, daysEarlier: 2 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const beforeDate = new Date().toISOString();
        const bd = { ...bucketData, details: {
            marker: 'key0',
            listType: 'current',
            prefix,
            beforeDate,
        } };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bd, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Marker, 'key0');
            assert.strictEqual(listLifecycleParams.Prefix, prefix);
            assert.strictEqual(listLifecycleParams.BeforeDate, beforeDate);

            assert.strictEqual(kafkaEntries.length, 0);
            return done();
        });
    });

    it('should publish one object with the bucketData details set', done => {
        const prefix = 'pre1';
        const keyName = `${prefix}key1`;
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 2 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const beforeDate = new Date().toISOString();
        const bd = { ...bucketData, details: {
            keyMarker: 'key0',
            versionIdMarker: 'versionid0',
            listType: 'noncurrent',
            prefix,
            beforeDate,
            storageClass: destinationLocation,
        } };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bd, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the non-current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.KeyMarker, 'key0');
            assert.strictEqual(listLifecycleParams.VersionIdMarker, 'versionid0');
            assert.strictEqual(listLifecycleParams.Prefix, prefix);
            assert.strictEqual(listLifecycleParams.BeforeDate, beforeDate);
            assert.strictEqual(listLifecycleParams.ExcludedDataStoreName, destinationLocation);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, versionId });
            return done();
        });
    });

    it('should publish one object with the bucketData details set for orphan', done => {
        const expirationRule = [
            {
                Expiration: { Days: 2 },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];

        const prefix = 'pre1';
        const keyName = `${prefix}key1`;
        const versionId = 'versionid1';
        const key = keyMock.orphanDeleteMarker({ keyName, versionId, daysEarlier: 2 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const beforeDate = new Date().toISOString();
        const bd = { ...bucketData, details: {
            marker: 'key0',
            listType: 'orphan',
            prefix,
            beforeDate,
        } };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(expirationRule, bd, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the orphan listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'orphan');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Marker, 'key0');
            assert.strictEqual(listLifecycleParams.Prefix, prefix);
            assert.strictEqual(listLifecycleParams.BeforeDate, beforeDate);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, versionId });
            return done();
        });
    });

    it('should publish one object entry if object is eligible', done => {
        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 2 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the non-current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            expectNominalListingParams(bucketName, listLifecycleParams);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, versionId });
            return done();
        });
    });

    it('should publish one object entry if object is eligible with NoncurrentVersionTransitions rule', done => {
        const transitionRule = [
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 2, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 2 });
        const { ETag, LastModified } = key;
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the non-current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, '');
            assert(!!listLifecycleParams.BeforeDate);
            assert.strictEqual(listLifecycleParams.ExcludedDataStoreName, destinationLocation);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
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

    it('should publish 0byte version transition to DMF with NoncurrentVersionTransitions rule', done => {
        const transitionRule = [
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 2, StorageClass: 'location-dmf-v1' }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 2, size: 0 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
            backbeatMetadataProxy, nbRetries, err => {
                assert.ifError(err);
                // test that the non-current listing is triggered
                assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');

                // test parameters used to list lifecycle keys
                const { listLifecycleParams } = backbeatMetadataProxy;
                assert.strictEqual(listLifecycleParams.Bucket, bucketName);
                assert.strictEqual(listLifecycleParams.Prefix, '');
                assert(!!listLifecycleParams.BeforeDate);
                assert.strictEqual(listLifecycleParams.ExcludedDataStoreName, 'location-dmf-v1');

                // test that the entry got pushed to kafka topic
                assert.strictEqual(kafkaEntries.length, 1);
                return done();
            });
    });

    it('should not publish any object entry if object is not eligible with NoncurrentVersionTransitions rule', done => {
        const transitionRule = [
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 2, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 1 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the non-current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, '');
            assert(!!listLifecycleParams.BeforeDate);
            assert.strictEqual(listLifecycleParams.ExcludedDataStoreName, destinationLocation);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 0);
            return done();
        });
    });

    it('should publish one bucket entry to list orphan delete markers if Expiration rule is set', done => {
        const expirationRule = [
            {
                Expiration: { Days: 2 },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 1 });
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
            testKafkaEntry.expectBucketEntry(firstEntry, {
                listType: 'orphan',
                hasBeforeDate: true,
                prefix: '',
            });
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
        const key = keyMock.current({ keyName, daysEarlier: 2 });
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
            assert(!!listLifecycleParams.BeforeDate);
            assert.strictEqual(listLifecycleParams.ExcludedDataStoreName, destinationLocation);

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

    it('should publish one transition object entry if object is eligible with later expiration', done => {
        const rules = [
            {
                Expiration: { Days: 3 },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 2, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 2 });
        const { ETag, LastModified } = key;

        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(rules, bucketData, s3,
            backbeatMetadataProxy, nbRetries, err => {
                assert.ifError(err);
                // test that the current listing is triggered
                assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

                // test parameters used to list lifecycle keys
                const { listLifecycleParams } = backbeatMetadataProxy;
                assert.strictEqual(listLifecycleParams.Bucket, bucketName);
                assert.strictEqual(listLifecycleParams.Prefix, '');
                assert(!!listLifecycleParams.BeforeDate);
                assert(listLifecycleParams.ExcludedDataStoreName === undefined);

                // test that the entry is valid and pushed to kafka topic
                assert.strictEqual(kafkaEntries.length, 2);
                const transitionEntry = kafkaEntries.find(entry => entry.topicName === dataMoverTopic);
                testKafkaEntry.expectObjectTransitionEntry(transitionEntry, {
                    keyName,
                    lastModified: LastModified,
                    eTag: ETag,
                    contentLength,
                    sourceLocation,
                    destinationLocation,
                });
                const bucketEntry = kafkaEntries.find(entry => entry.topicName === bucketTopic);
                testKafkaEntry.expectBucketEntry(bucketEntry, {
                    hasBeforeDate: true,
                    prefix: '',
                    listType: 'orphan',
                });
                return done();
            });
    });

    it('should publish one expiration object entry if object is eligible with later transition', done => {
        const rules = [
            {
                Expiration: { Days: 2 },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 3, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 2 });
        const { LastModified } = key;

        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(rules, bucketData, s3,
            backbeatMetadataProxy, nbRetries, err => {
                assert.ifError(err);
                // test that the current listing is triggered
                assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'current');

                // test parameters used to list lifecycle keys
                const { listLifecycleParams } = backbeatMetadataProxy;
                assert.strictEqual(listLifecycleParams.Bucket, bucketName);
                assert.strictEqual(listLifecycleParams.Prefix, '');
                assert(!!listLifecycleParams.BeforeDate);
                assert(listLifecycleParams.ExcludedDataStoreName === undefined);

                // test that the entry is valid and pushed to kafka topic
                assert.strictEqual(kafkaEntries.length, 2);
                const expirationEntry = kafkaEntries.find(entry => entry.topicName === objectTopic);
                testKafkaEntry.expectObjectExpirationEntry(expirationEntry, {
                    keyName,
                    lastModified: LastModified,
                });
                const bucketEntry = kafkaEntries.find(entry => entry.topicName === bucketTopic);
                testKafkaEntry.expectBucketEntry(bucketEntry, {
                    hasBeforeDate: true,
                    prefix: '',
                    listType: 'orphan',
                });
                return done();
            });
    });

    it('should publish 0byte object version transition to DMF', done => {
        const transitionRule = [
            {
                Transitions: [{ Days: 2, StorageClass: 'location-dmf-v1' }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const key = keyMock.current({ keyName, daysEarlier: 2, size: 0 });
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
                assert(!!listLifecycleParams.BeforeDate);
                assert.strictEqual(listLifecycleParams.ExcludedDataStoreName, 'location-dmf-v1');

                // test that the entry got pushed to kafka topic
                assert.strictEqual(kafkaEntries.length, 1);
                return done();
            });
    });


    it('should not expire delete marker if not old enough to satisfy the age criteria', done => {
        const expitationRule = [
            {
                Expiration: { Days: 10 },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'deletemarker1';
        const versionId = 'versionid1';
        const key = keyMock.orphanDeleteMarker({ keyName, versionId, daysEarlier: 2 });

        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const beforeDate = new Date().toISOString();
        const bd = { ...bucketData, details: {
            listType: 'orphan',
            beforeDate,
            prefix: '',
        } };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(expitationRule, bd, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the orphan listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'orphan');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, '');
            assert.strictEqual(listLifecycleParams.BeforeDate, beforeDate);

            assert.strictEqual(kafkaEntries.length, 0);
            return done();
        });
    });

    it('should expire delete marker if not old enough but ExpiredObjectDeleteMarker is set to true', done => {
        const expitationRule = [
            {
                Expiration: { Days: 10 },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            },
            {
                Expiration: { ExpiredObjectDeleteMarker: true },
                ID: '456',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'deletemarker1';
        const versionId = 'versionid1';
        const key = keyMock.orphanDeleteMarker({ keyName, versionId, daysEarlier: 2 });

        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const beforeDate = new Date().toISOString();
        const bd = { ...bucketData, details: {
            listType: 'orphan',
            beforeDate,
            prefix: '',
        } };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(expitationRule, bd, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the orphan listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'orphan');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, '');
            assert.strictEqual(listLifecycleParams.BeforeDate, beforeDate);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, versionId });
            return done();
        });
    });

    it('should publish one object entry if object is eligible with ExpiredObjectDeleteMarker', done => {
        const expitationRule = [
            {
                Expiration: { ExpiredObjectDeleteMarker: true },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'deletemarker1';
        const versionId = 'versionid1';
        const key = keyMock.orphanDeleteMarker({ keyName, versionId, daysEarlier: 2 });

        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(expitationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the orphan listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'orphan');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, '');
            assert(!listLifecycleParams.BeforeDate);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, versionId });
            return done();
        });
    });

    it('should publish one bucket entry if listing is trucated', done => {
        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, daysEarlier: 1 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse =
            { contents, isTruncated: true, markerInfo: { keyMarker: keyName, versionIdMarker: versionId } };

        const nbRetries = 0;
        lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the non-current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            expectNominalListingParams(bucketName, listLifecycleParams);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectBucketEntry(firstEntry, {
                hasBeforeDate: true,
                keyMarker: keyName,
                versionIdMarker: versionId,
                prefix: '',
                listType: 'noncurrent',
            });
            return done();
        });
    });

    it('should publish one bucket entry if listing keys to be transitioned is truncated', done => {
        const transitionRule = [
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 2, StorageClass: destinationLocation }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 1 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse =
            { contents, isTruncated: true, markerInfo: { keyMarker: keyName, versionIdMarker: versionId } };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(transitionRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the non-current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, '');
            assert(!!listLifecycleParams.BeforeDate);
            assert.strictEqual(listLifecycleParams.ExcludedDataStoreName, destinationLocation);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectBucketEntry(firstEntry, {
                hasBeforeDate: true,
                keyMarker: keyName,
                versionIdMarker: versionId,
                prefix: '',
                listType: 'noncurrent',
                storageClass: destinationLocation,
            });
            return done();
        });
    });

    it('should publish one bucket and one object entry if object is elligible and listing is trucated', done => {
        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 2 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse =
            { contents, isTruncated: true, markerInfo: { marker: keyName } };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            assert.strictEqual(kafkaEntries.length, 2);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            expectNominalListingParams(bucketName, listLifecycleParams);

            // test that the entry is valid and pushed to kafka topic
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectBucketEntry(firstEntry, {
                hasBeforeDate: true,
                marker: keyName,
                prefix: '',
                listType: 'noncurrent',
            });

            const secondEntry = kafkaEntries[1];
            testKafkaEntry.expectObjectExpirationEntry(secondEntry, { keyName, versionId });
            return done();
        });
    });

    it('should publish one bucket entry if multiple rules', done => {
        const multipleRules = [
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 2 },
                ID: '0',
                Prefix: 'pre1',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 3, StorageClass: destinationLocation }],
                ID: '1',
                Prefix: 'pre2',
                Status: 'Enabled',
            }
        ];
        const keyName = 'pre1-key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 1 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(multipleRules, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');
            const { listLifecycleParams } = backbeatMetadataProxy;
            assert.strictEqual(listLifecycleParams.Bucket, bucketName);
            assert.strictEqual(listLifecycleParams.Prefix, 'pre1');
            assert(!!listLifecycleParams.BeforeDate);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectBucketEntry(firstEntry, {
                listType: 'noncurrent',
                hasBeforeDate: true,
                prefix: 'pre2',
                storageClass: destinationLocation,
            });
            return done();
        });
    });

    it('should not publish any entry because version is retain by NewerNoncurrentVersions', done => {
        const nonCurrentExpirationRule = [{
            NoncurrentVersionExpiration: {
                NoncurrentDays: 2,
                NewerNoncurrentVersions: 1,
            },
            ID: '123',
            Prefix: '',
            Status: 'Enabled',
        }];

        const keyName = 'key1';
        const versionId = 'versionid1';
        const key = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 2 });
        const contents = [key];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the non-current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            expectNominalListingParams(bucketName, listLifecycleParams);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 0);
            return done();
        });
    });

    it('should retain the first non current version but publish the second one', done => {
        const nonCurrentExpirationRule = [{
            NoncurrentVersionExpiration: {
                NoncurrentDays: 2,
                NewerNoncurrentVersions: 1,
            },
            ID: '123',
            Prefix: '',
            Status: 'Enabled',
        }];

        const keyName = 'key1';
        const versionId = 'versionid1';
        const version1 = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 3 });
        const versionId2 = 'versionid2';
        const version2 = keyMock.nonCurrent({ keyName, versionId: versionId2, daysEarlier: 2 });
        const contents = [version1, version2];
        backbeatMetadataProxy.listLifecycleResponse = { contents, isTruncated: false, markerInfo: {} };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);
            // test that the non-current listing is triggered
            assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');

            // test parameters used to list lifecycle keys
            const { listLifecycleParams } = backbeatMetadataProxy;
            expectNominalListingParams(bucketName, listLifecycleParams);

            // test that the entry is valid and pushed to kafka topic
            assert.strictEqual(kafkaEntries.length, 1);
            const firstEntry = kafkaEntries[0];
            testKafkaEntry.expectObjectExpirationEntry(firstEntry, { keyName, versionId });
            return done();
        });
    });

    it('should retain the correct number of versions when the rule changes', done => {
        const nonCurrentExpirationRule = [{
            NoncurrentVersionExpiration: {
                NoncurrentDays: 2,
                NewerNoncurrentVersions: 2,
            },
            ID: '123',
            Prefix: '',
            Status: 'Enabled',
        }];

        const keyName = 'key1';
        const versionId = 'versionid1';
        const version1 = keyMock.nonCurrent({ keyName, versionId, daysEarlier: 3 });
        const versionId2 = 'versionid2';
        const version2 = keyMock.nonCurrent({ keyName, versionId: versionId2, daysEarlier: 2 });
        const contents = [version1, version2];
        backbeatMetadataProxy.listLifecycleResponse = {
            contents,
            isTruncated: true,
            markerInfo: { keyMarker: 'key1', versionIdMarker: 'versionid2' },
        };

        const nbRetries = 0;
        return lifecycleTask.processBucketEntry(nonCurrentExpirationRule, bucketData, s3,
        backbeatMetadataProxy, nbRetries, err => {
            assert.ifError(err);

            kafkaEntries = [];
            const nonCurrentExpirationRule2 = [{
                NoncurrentVersionExpiration: {
                    NoncurrentDays: 2,
                    NewerNoncurrentVersions: 1,
                },
                ID: '456',
                Prefix: '',
                Status: 'Enabled',
            }];

            return lifecycleTask.processBucketEntry(nonCurrentExpirationRule2, bucketData, s3,
                backbeatMetadataProxy, nbRetries, err => {
                    assert.ifError(err);
                    // test that the non-current listing is triggered
                    assert.strictEqual(backbeatMetadataProxy.listLifecycleType, 'noncurrent');

                    // test parameters used to list lifecycle keys
                    const { listLifecycleParams } = backbeatMetadataProxy;
                    expectNominalListingParams(bucketName, listLifecycleParams);

                    // test that the entry is valid and pushed to kafka topic
                    assert.strictEqual(kafkaEntries.length, 2);

                    const firstEntry = kafkaEntries[0];
                    testKafkaEntry.expectBucketEntry(firstEntry, {
                        hasBeforeDate: true,
                        keyMarker: keyName,
                        versionIdMarker: versionId2,
                        prefix: '',
                        listType: 'noncurrent',
                    });

                    const secondEntry = kafkaEntries[1];
                    testKafkaEntry.expectObjectExpirationEntry(secondEntry, { keyName, versionId });
                    return done();
                });
        });
    });
});
