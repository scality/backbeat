'use strict'; // eslint-disable-line

const werelogs = require('werelogs');
const { ObjectMD } = require('arsenal').models;

const LifecycleObjectExpirationProcessor = require(
    '../../../extensions/lifecycle/objectProcessor/LifecycleObjectExpirationProcessor');

const { S3ClientMock } = require('../../utils/S3ClientMock');
const { BackbeatClientMock } = require('../../utils/BackbeatClientMock');


const {
    zkConfig,
    kafkaConfig,
    lcConfig,
    s3Config,
    objectTasksTopic,
    testTimeout,
} = require('./configObjects');

werelogs.configure({ level: 'warn', dump: 'error' });

const backbeatRoutes = ['deleteObject'];

describe('Lifecycle Object Processor', () => {
    jest.setTimeout(testTimeout);

    function generateRetryTest(name, failures, message) {
        return function testRetries(done) {
            const lop = new LifecycleObjectExpirationProcessor(
                zkConfig, kafkaConfig, lcConfig, s3Config);

            const s3Client = new S3ClientMock(failures);
            const backbeatClient = new BackbeatClientMock(failures);
            lop.clientManager.getS3Client = () => s3Client;
            lop.clientManager.getBackbeatClient = () => backbeatClient;
            lop.clientManager.getBackbeatMetadataProxy = () => ({
                getMetadata: (_a, _b, cb) =>
                    cb(null, { Body: new ObjectMD().getSerialized() }),
            });

            lop.start(err => {
                if (err) {
                    return done(err);
                }

                const objectTaskConsumer =
                    lop._consumers.getConsumer(lcConfig.objectTasksTopic);

                objectTaskConsumer.onEntryCommittable = () => {
                    if (backbeatRoutes.includes(name)) {
                        backbeatClient.verifyRetries();
                    } else {
                        s3Client.verifyRetries();
                    }
                    done();
                };

                let messagesToConsume = [message];
                objectTaskConsumer._consumer.consume = (_, cb) => {
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
                deleteObjectFromExpiration: 2,
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
            generateRetryTest(testCase.name, testCase.failures, testCase.message));
    });
});
