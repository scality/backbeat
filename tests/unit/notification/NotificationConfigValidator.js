const assert = require('assert');

const configValidator = require('../../../extensions/notification/NotificationConfigValidator');

describe('NotificationConfigValidator ::', () => {
    it('should throw an error if requiredAcks is specified for a non-kafka destination', () => {
        const extConfig = {
            topic: 'topic',
            monitorNotificationFailures: true,
            notificationFailedTopic: 'failed-topic',
            queueProcessor: {
                groupId: 'groupId',
                concurrency: 1000,
            },
            destinations: [{
                resource: 'resource',
                type: 'other',
                host: 'host',
                port: 8000,
                topic: 'topic',
                requiredAcks: 1,
            }],
            probeServer: {
                bindAddress: 'localhost',
                port: 8000,
            },
        };
        assert.throws(() => configValidator(null, extConfig));
    });

    it('should throw an error if compressionType is specified for a non-kafka destination', () => {
        const extConfig = {
            topic: 'topic',
            monitorNotificationFailures: true,
            notificationFailedTopic: 'failed-topic',
            queueProcessor: {
                groupId: 'groupId',
                concurrency: 1000,
            },
            destinations: [{
                resource: 'resource',
                type: 'other',
                host: 'host',
                port: 8000,
                topic: 'topic',
                compressionType: 'none',
            }],
            probeServer: {
                bindAddress: 'localhost',
                port: 8000,
            },
        };
        assert.throws(() => configValidator(null, extConfig));
    });

    it('should not throw an error if requiredAcks and compressionType is specified for a kafka destination', () => {
        const extConfig = {
            topic: 'topic',
            monitorNotificationFailures: true,
            notificationFailedTopic: 'failed-topic',
            queueProcessor: {
                groupId: 'groupId',
                concurrency: 1000,
            },
            destinations: [{
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                requiredAcks: 1,
                compressionType: 'none',
            }],
            probeServer: {
                bindAddress: 'localhost',
                port: 8000,
            },
        };
        assert.doesNotThrow(() => configValidator(null, extConfig));
    });
});
