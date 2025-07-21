const assert = require('assert');

const { NotificationConfigValidator } = require('../../../extensions/notification/NotificationConfigValidator');

const defaultExtConfig = {
    topic: 'topic',
    monitorNotificationFailures: true,
    notificationFailedTopic: 'failed-topic',
    queueProcessor: {
        groupId: 'groupId',
        concurrency: 1000,
    },
    destinations: [],
    probeServer: {
        bindAddress: 'localhost',
        port: 8000,
    },
};

describe('NotificationConfigValidator ::', () => {
    const testCases = [
        {
            valid: false,
            description: 'requiredAcks specified for a non-kafka destination',
            destinationConfig: {
                resource: 'resource',
                type: 'other',
                host: 'host',
                port: 8000,
                topic: 'topic',
                requiredAcks: 1,
            }
        },
        {
            valid: false,
            description: 'compressionType specified for a non-kafka destination',
            destinationConfig: {
                resource: 'resource',
                type: 'other',
                host: 'host',
                port: 8000,
                topic: 'topic',
                compressionType: 'none',
            },
        },
        {
            valid: true,
            description: 'requiredAcks and compressionType specified for a kafka destination',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                requiredAcks: 1,
                compressionType: 'none',
            },
        },
        {
            valid: true,
            description: 'kerberos auth',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'kerberos',
                    protocol: 'SASL_PLAINTEXT',
                    keytab: 'path/to/keytab',
                    principal: 'my-principal',
                    serviceName: 'kafka',
                }
            },
        },
        {
            valid: false,
            description: 'kerberos auth no protocol',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'kerberos',
                    keytab: 'path/to/keytab',
                    principal: 'my-principal',
                    serviceName: 'kafka',
                }
            },
        },
        {
            valid: false,
            description: 'kerberos auth invalid protocol',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'kerberos',
                    protocol: 'INVALID_PROTOCOL',
                    keytab: 'path/to/keytab',
                    principal: 'my-principal',
                    serviceName: 'kafka',
                }
            },
        },
        {
            valid: false,
            description: 'kerberos auth no keytab',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'kerberos',
                    protocol: 'SASL_PLAINTEXT',
                    principal: 'my-principal',
                    serviceName: 'kafka',
                }
            },
        },
        {
            valid: false,
            description: 'kerberos auth no principal',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'kerberos',
                    protocol: 'SASL_PLAINTEXT',
                    keytab: 'path/to/keytab',
                    serviceName: 'kafka',
                }
            },
        },
        {
            valid: false,
            description: 'kerberos auth no serviceName',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'kerberos',
                    protocol: 'SASL_PLAINTEXT',
                    keytab: 'path/to/keytab',
                    principal: 'my-principal',
                }
            },
        },
        {
            valid: true,
            description: 'basic auth',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'basic',
                    protocol: 'SASL_PLAINTEXT',
                    credentialsFile: 'path/to/credentials',
                }
            },
        },
        {
            valid: false,
            description: 'basic auth no credentialsFile',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'basic',
                    protocol: 'SASL_PLAINTEXT',
                }
            },
        },
        {
            valid: true,
            description: 'empty auth',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {},
            },
        },
        {
            valid: true,
            description: 'ssl only',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    ssl: true,
                    ca: 'path/to/ca',
                    client: 'path/to/client',
                    key: 'path/to/key',
                    keyPassword: 'key-password',
                },
            },
        },
    ];

    testCases.forEach(testCase =>
        it(`[${testCase.valid ? 'VALID' : 'INVALID'}] ${testCase.description}`, () => {
            const extConfig = {
                ...defaultExtConfig,
                destinations: [testCase.destinationConfig],
            };
            const tester = testCase.valid ? assert.doesNotThrow : assert.throws;
            tester(() => NotificationConfigValidator(null, extConfig));
        })
    );
});
