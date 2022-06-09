'use strict'; // eslint-disable-line

const assert = require('assert');
const werelogs = require('werelogs');
const { errors } = require('arsenal');

const LifecycleBucketProcessor = require(
    '../../../extensions/lifecycle/bucketProcessor/LifecycleBucketProcessor');

const { S3ClientMock } = require('../../utils/S3ClientMock');

const {
    zkConfig,
    kafkaConfig,
    lcConfig,
    repConfig,
    s3Config,
    bucketTasksTopic,
    objectTasksTopic,
    testTimeout,
} = require('./configObjects');

const bucketEntryMessage = {
    key: '12345',
    topic: bucketTasksTopic,
    partition: 0,
    offset: 0,
    timestamp: 1633382688726,
    value: `{
        "action": "processObjects",
        "target": {
            "bucket": "bucket1",
            "owner": "owner1",
            "accountId": "acct1"
        },
        "details": {
        }
    }`,
};

werelogs.configure({ level: 'warn', dump: 'error' });

describe('Lifecycle Bucket Processor', function lifecycleBucketProcessor() {
    this.timeout(testTimeout);

    function generateRetryTest(s3Client, shouldRetry = true) {
        const lbp = new LifecycleBucketProcessor(
            zkConfig, kafkaConfig, lcConfig, repConfig, s3Config);

        lbp.clientManager.getS3Client = () => s3Client;
        lbp.clientManager.getBackbeatClient = () => ({});
        lbp.clientManager.getBackbeatMetadataProxy = () => ({});

        const start = new Promise((resolve, reject) => {
            lbp.start(err => {
                if (err) {
                    reject(err);
                } else {
                    let messagesToConsume = [bucketEntryMessage];
                    lbp._consumer._consumer.consume = (_, cb) => {
                        process.nextTick(cb, null, messagesToConsume);
                        messagesToConsume = [];
                    };

                    resolve();
                }
            });
        });

        return new Promise((resolve, reject) => {
            const messages = [];
            start.then(() => {
                lbp._producer.sendToTopic = (topic, [{ message }], cb) => {
                    const entry = JSON.parse(message);
                    messages.push({ topic, entry });
                    if (cb) {
                        process.nextTick(cb);
                    }
                    return;
                };
                lbp._consumer.onEntryCommittable = () => {
                    if (shouldRetry) {
                        s3Client.verifyRetries();
                    } else {
                        s3Client.verifyNoRetries();
                    }
                    resolve(messages);
                };
            })
            .catch(reject);
        });
    }

    [
        {
            // S3 API call from inside the bucket processor's async queue
            name: 'listObjects',
            failures: {
                listObjects: 2,
            },
        },
        {
            // S3 API call from inside the bucket processor's async queue
            name: 'headObject',
            failures: {
                listObjects: 2,
            },
        },
        {
            // S3 API call from outside of the bucket processor's async queue,
            // invoked indirectly by the `BackbeatConsumer` instance before
            // dispatching to the async queue
            name: 'getBucketLifecycleConfiguration',
            failures: {
                getBucketLifecycleConfiguration: 4,
            },
        },
    ].forEach(testCase => {
        it(`should retry bucket entries when ${testCase.name} fails`, () => {
            const s3Client = new S3ClientMock(testCase.failures);
            return generateRetryTest(s3Client).then(messages => {
                assert.strictEqual(messages.length, 1);

                const message = messages[0];
                assert.strictEqual(message.topic, objectTasksTopic);
                assert.strictEqual(message.entry.action, 'deleteObject');
                assert.deepStrictEqual(message.entry.target, {
                    owner: 'owner1',
                    bucket: 'bucket1',
                    accountId: 'acct1',
                    key: 'obj1',
                });
            });
        });
    });

    // If the listing is not complete, the lifecycleBucketProcessor publishes a new entry
    // to the backbeat-lifecycle-bucket-tasks topic with the action name: processObjects.
    // It should happen only once even if process is retrying because of failures.
    it('should only requeue batch of objects once if retryable failures occurred', () => {
        const failures = { getObjectTagging: 2 };
        const s3Client = new S3ClientMock(failures);
        s3Client.stubGetBucketLcWithTag().stubListObjectsTruncated();

        return generateRetryTest(s3Client).then(messages => {
            const actions = messages.map(m => m.entry.action);
            // "processObjects" (requeue) action entry should only be sent once before
            // the process succeeds.
            const expectedActions = ['processObjects', 'deleteObject'];

            assert.deepStrictEqual(actions, expectedActions);
        });
    });

    it('should only requeue batch of versions once if retryable failures occurred', () => {
        const failures = { getObjectTagging: 2 };
        const s3Client = new S3ClientMock(failures);
        s3Client.stubMethod('getBucketVersioning', { Status: 'Enabled' });
        s3Client.stubGetBucketLcWithTag().stubListVersionsTruncated();

        return generateRetryTest(s3Client).then(messages => {
            const actions = messages.map(m => m.entry.action);
            // "processObjects" (requeue) action entry should only be sent once before
            // the process succeeds.
            const expectedActions = ['processObjects', 'deleteObject'];

            assert.deepStrictEqual(actions, expectedActions);
        });
    });

    it.only('should not retry for NoSuchBucket errors', () => {
        const s3Client = new S3ClientMock({ getBucketLifecycleConfiguration: 2 });
        s3Client.stubMethod('getBucketLifecycleConfiguration', null, errors.NoSuchBucket);

        return generateRetryTest(s3Client, false).then(messages => {
            assert.deepStrictEqual(messages, []);
            assert.deepStrictEqual(s3Client.calls.getBucketLifecycleConfiguration, 1);
        });
    });

    it('should only requeue batch of MPUs once if retryable failures occurred', () => {
        const failures = { getObjectTagging: 2 };
        const s3Client = new S3ClientMock(failures);
        s3Client.stubGetBucketLcWithTag().stubListObjectsTruncated().stubListMpuTruncated();

        return generateRetryTest(s3Client).then(messages => {
            const actions = messages.map(m => m.entry.action);
            // "processObjects" (requeue) action entry sent twice, one for MPUs batch
            // and one for objects batch.
            const expectedActions = ['processObjects', 'processObjects', 'deleteObject'];

            assert.deepStrictEqual(actions, expectedActions);

            // check that "MPU batch" action entry is only sent once before
            // the process succeeds.
            const reprocessMPUActions = messages
                .filter(m =>
                m.entry.action === 'processObjects' && m.entry.details.keyMarker === 'mpu2');

            assert.strictEqual(reprocessMPUActions.length, 1);
        });
    });
});
