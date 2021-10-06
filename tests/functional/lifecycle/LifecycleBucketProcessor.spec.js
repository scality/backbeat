'use strict'; // eslint-disable-line

const assert = require('assert');
const werelogs = require('werelogs');

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

    function generateRetryTest(failures) {
        return function testRetries() {
            const lbp = new LifecycleBucketProcessor(
                zkConfig, kafkaConfig, lcConfig, repConfig, s3Config);

            const s3Client = new S3ClientMock(failures);
            lbp._getS3Client = () => s3Client;
            lbp._getBackbeatClient = () => ({});

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

            const destTopicSendAssert = new Promise((resolve, reject) => {
                start
                .then(() => {
                    lbp._producer.sendToTopic = (topic, [{ message }]) => {
                        const obj = JSON.parse(message);

                        assert.strictEqual(topic, objectTasksTopic);
                        assert.strictEqual(obj.action, 'deleteObject');
                        assert.deepStrictEqual(obj.target, {
                            owner: 'owner1',
                            bucket: 'bucket1',
                            accountId: 'acct1',
                            key: 'obj1',
                        });

                        resolve();
                    };
                })
                .catch(reject);
            });

            const sourceTopicCommitAssert = new Promise((resolve, reject) => {
                start
                .then(() => {
                    lbp._consumer.onEntryCommittable = () => {
                        s3Client.verifyRetries();
                        resolve();
                    };
                })
                .catch(reject);
            });

            return Promise.all([
                sourceTopicCommitAssert,
                destTopicSendAssert,
            ]);
        };
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
        it(`should retry bucket entries when ${testCase.name} fails`,
            generateRetryTest(testCase.failures));
    });
});
