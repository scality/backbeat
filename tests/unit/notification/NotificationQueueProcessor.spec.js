const assert = require('assert');
const werelogs = require('werelogs');
const sinon = require('sinon');

const NotificationQueueProcessor = require('../../../extensions/notification/queueProcessor/QueueProcessor');
const constants = require('../../../extensions/notification/constants');

const mongoConfig
    = require('../../config.notification.json').queuePopulator.mongo;
const kafkaConfig
    = require('../../config.notification.json').queuePopulator.kafka;
const notificationConfig
    = require('../../config.notification.json').extensions.notification;

const logger = new werelogs.Logger('NotificationQueueProcessor:test');

const notificationConfiguration = {
    queueConfig: [
        {
            events: ['s3:ObjectCreated:*'],
            queueArn: 'arn:scality:bucketnotif:::destination1',
            filterRules: [],
        },
    ],
};

const kafkaEntry = {
    value: JSON.stringify({
        dateTime: '2022-09-16T09:42:18.331Z',
        eventType: 's3:ObjectCreated:Put',
        region: 'us-east-1',
        schemaVersion: '6',
        size: '0',
        versionId: null,
        bucket: 'tmp-bucket',
        key: 'example-object',
    })
};

const sentEntry = JSON.stringify({
    Records: [
        {
            eventVersion: constants.eventVersion,
            eventSource: constants.eventSource,
            awsRegion: 'us-east-1',
            eventTime: '2022-09-16T09:42:18.331Z',
            eventName: 's3:ObjectCreated:Put',
            userIdentity: {
                principalId: null,
            },
            requestParameters: {
                sourceIPAddress: null,
            },
            responseElements: {
                'x-amz-request-id': null,
                'x-amz-id-2': null,
            },
            s3: {
                s3SchemaVersion: constants.eventS3SchemaVersion,
                bucket: {
                    name: 'tmp-bucket',
                    ownerIdentity: {
                        principalId: null,
                    },
                    arn: null,
                },
                object: {
                    key: 'example-object',
                    size: '0',
                    eTag: null,
                    versionId: null,
                    sequencer: null,
                },
            },
        },
    ],
});

describe('NotificationQueueProcessor:: ', () => {
    let notificationQueueProcessor;

    beforeEach(() => {
        notificationQueueProcessor = new NotificationQueueProcessor(mongoConfig, kafkaConfig,
            notificationConfig, notificationConfig.destinations[0].resource, null);
        notificationQueueProcessor.logger = logger;
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('processKafkaEntry ::', () => {
        it('Should publish notification in correct format', async () => {
            notificationQueueProcessor._getConfig = sinon.stub().yields(null, notificationConfiguration);
            const sendStub = sinon.stub().yields(null);
            notificationQueueProcessor._destination = {
                send: sendStub,
            };
            notificationQueueProcessor.processKafkaEntry(kafkaEntry, err => {
                assert.ifError(err);
                const expectedMessage = [{
                    key: 'tmp-bucket/example-object',
                    message: sentEntry,
                }];
                assert.deepStrictEqual(sendStub.args[0][0], expectedMessage);
            });
        });
    });
});
