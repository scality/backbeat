'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const LifecycleObjectExpirationProcessor = require(
    '../../../extensions/lifecycle/objectProcessor/LifecycleObjectExpirationProcessor');

const { S3ClientMock } = require('../../utils/S3ClientMock');

const {
    zkConfig,
    kafkaConfig,
    lcConfig,
    s3Config,
    objectTasksTopic,
    testTimeout,
} = require('./configObjects');

werelogs.configure({ level: 'warn', dump: 'error' });

describe('Lifecycle Object Processor', function lifecycleObjectProcessor() {
    this.timeout(testTimeout);

    function generateRetryTest(failures, message) {
        return function testRetries(done) {
            const lop = new LifecycleObjectExpirationProcessor(
                zkConfig, kafkaConfig, lcConfig, s3Config);

            const s3Client = new S3ClientMock(failures);
            lop._getS3Client = () => s3Client;

            lop.start(err => {
                if (err) {
                    return done(err);
                }

                lop._consumer.onEntryCommittable = () => {
                    s3Client.verifyRetries();
                    done();
                };

                let messagesToConsume = [message];
                lop._consumer._consumer.consume = (_, cb) => {
                    process.nextTick(cb, null, messagesToConsume);
                    messagesToConsume = [];
                };

                return undefined;
            });
        };
    }

    [
        {
            name: 'deleteObject',
            failures: {
                deleteObject: 2,
            },
            message: {
                key: '12345',
                topic: objectTasksTopic,
                partition: 0,
                offset: 0,
                timestamp: 1633382688726,
                value: `{
                    "action": "deleteObject",
                    "target": {
                        "bucket": "bucket1",
                        "key": "obj1",
                        "owner": "owner1",
                        "accountId": "acct1"
                    },
                    "details": {
                    }
                }`,
            },
        },
        {
            name: 'abortMultipartUpload',
            failures: {
                abortMultipartUpload: 2,
            },
            message: {
                key: '12345',
                topic: objectTasksTopic,
                partition: 0,
                offset: 0,
                timestamp: 1633382688726,
                value: `{
                    "action": "deleteMPU",
                    "target": {
                        "bucket": "bucket1",
                        "key": "obj1",
                        "owner": "owner1",
                        "accountId": "acct1"
                    },
                    "details": {
                    }
                }`,
            },
        },
    ].forEach(testCase => {
        it(`should retry object entries when ${testCase.name} fails`,
            generateRetryTest(testCase.failures, testCase.message));
    });
});
