const assert = require('assert');

const { notificationConfigValidator } = require('../../../extensions/notification/NotificationConfigValidator');

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
            description: 'basic auth credentialsFile',
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
            valid: true,
            description: 'basic auth username/password',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'basic',
                    protocol: 'SASL_PLAINTEXT',
                    username: 'foo',
                    password: 'bar',
                }
            },
        },
        {
            valid: false,
            description: 'basic auth missing credentials',
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
            valid: false,
            description: 'basic auth empty password',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'basic',
                    protocol: 'SASL_PLAINTEXT',
                    username: 'foo',
                    password: '',
                }
            },
        },
        {
            valid: false,
            description: 'basic auth unset username',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'basic',
                    protocol: 'SASL_PLAINTEXT',
                    password: 'bar',
                }
            },
        },
        {
            valid: false,
            description: 'basic auth inline credentials and credentials file',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'basic',
                    protocol: 'SASL_PLAINTEXT',
                    credentialsFile: 'credentials.json',
                    username: 'testuser',
                    password: 'testpassword',
                }
            },
        },
        {
            valid: false,
            description: 'basic auth empty credentials file',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'basic',
                    protocol: 'SASL_PLAINTEXT',
                    credentialsFile: '',
                }
            },
        },
        // SCRAM auth
        {
            valid: true,
            description: 'scram auth credentialsFile',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'scram',
                    protocol: 'SASL_SSL',
                    mechanism: 'SHA-256',
                    credentialsFile: 'path/to/credentials',
                }
            },
        },
        {
            valid: true,
            description: 'scram auth username/password',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'scram',
                    protocol: 'SASL_PLAINTEXT',
                    mechanism: 'SHA-512',
                    username: 'foo',
                    password: 'bar',
                }
            },
        },
        {
            valid: false,
            description: 'scram auth missing credentials',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'scram',
                    protocol: 'SASL_PLAINTEXT',
                    mechanism: 'SHA-256',
                }
            },
        },
        {
            valid: false,
            description: 'scram auth empty password',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'scram',
                    protocol: 'SASL_PLAINTEXT',
                    mechanism: 'SHA-256',
                    username: 'foo',
                    password: '',
                }
            },
        },
        {
            valid: false,
            description: 'scram auth unset username',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'scram',
                    protocol: 'SASL_PLAINTEXT',
                    mechanism: 'SHA-256',
                    password: 'bar',
                }
            },
        },
        {
            valid: false,
            description: 'scram auth inline credentials and credentials file',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'scram',
                    protocol: 'SASL_PLAINTEXT',
                    mechanism: 'SHA-256',
                    credentialsFile: 'credentials.json',
                    username: 'testuser',
                    password: 'testpassword',
                }
            },
        },
        {
            valid: false,
            description: 'scram auth empty credentialsFile',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'scram',
                    protocol: 'SASL_PLAINTEXT',
                    mechanism: 'SHA-256',
                    credentialsFile: '',
                }
            },
        },
        {
            valid: false,
            description: 'scram auth missing mechanism',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'scram',
                    protocol: 'SASL_PLAINTEXT',
                    username: 'foo',
                    password: 'bar',
                }
            },
        },
        {
            valid: false,
            description: 'scram auth invalid mechanism',
            destinationConfig: {
                resource: 'resource',
                type: 'kafka',
                host: 'host',
                port: 8000,
                topic: 'topic',
                auth: {
                    type: 'scram',
                    protocol: 'SASL_PLAINTEXT',
                    mechanism: 'SHA-1',
                    username: 'foo',
                    password: 'bar',
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
            tester(() => notificationConfigValidator(null, extConfig));
        })
    );
});

describe('NotificationConfigValidator delivery pool ::', () => {
    const destinationConfig = {
        resource: 'resource',
        type: 'kafka',
        host: 'host',
        port: 8000,
        topic: 'topic',
    };

    it('should default the destination spread factor to 1', () => {
        const config = notificationConfigValidator(null, {
            ...defaultExtConfig,
            destinations: [destinationConfig],
        });
        assert.strictEqual(config.destinations[0].spreadFactor, 1);
    });

    it('should reject a spread factor below 1', () => {
        assert.throws(() => notificationConfigValidator(null, {
            ...defaultExtConfig,
            destinations: [{ ...destinationConfig, spreadFactor: 0 }],
        }));
    });

    it('should reject a non integer spread factor', () => {
        assert.throws(() => notificationConfigValidator(null, {
            ...defaultExtConfig,
            destinations: [{ ...destinationConfig, spreadFactor: 1.5 }],
        }));
    });

    it('should leave the delivery pool unset when it is not configured', () => {
        const config = notificationConfigValidator(null, defaultExtConfig);
        assert.strictEqual(config.deliveryPool, undefined);
    });

    it('should apply the delivery pool defaults', () => {
        const config = notificationConfigValidator(null, {
            ...defaultExtConfig,
            deliveryPool: {},
        });
        assert.strictEqual(config.deliveryPool.enabled, false);
        assert.strictEqual(config.deliveryPool.deliveryTimeoutMs, 30000);
        assert.strictEqual(config.deliveryPool.producerIdleMs, 300000);
        assert.strictEqual(config.deliveryPool.maxProducers, 50);
        assert.strictEqual(config.deliveryPool.concurrency, 1000);
        assert.strictEqual(config.deliveryPool.maxQueued, 1000);
    });

    it('should accept an enabled delivery pool with a topic and a group id', () => {
        assert.doesNotThrow(() => notificationConfigValidator(null, {
            ...defaultExtConfig,
            deliveryPool: {
                enabled: true,
                topic: 'delivery-topic',
                groupId: 'delivery-group',
            },
        }));
    });

    it('should require a topic when the delivery pool is enabled', () => {
        assert.throws(() => notificationConfigValidator(null, {
            ...defaultExtConfig,
            deliveryPool: {
                enabled: true,
                groupId: 'delivery-group',
            },
        }));
    });

    it('should require a group id when the delivery pool is enabled', () => {
        assert.throws(() => notificationConfigValidator(null, {
            ...defaultExtConfig,
            deliveryPool: {
                enabled: true,
                topic: 'delivery-topic',
            },
        }));
    });

    it('should reject a delivery timeout below the producer request timeout', () => {
        assert.throws(() => notificationConfigValidator(null, {
            ...defaultExtConfig,
            deliveryPool: {
                enabled: true,
                topic: 'delivery-topic',
                groupId: 'delivery-group',
                deliveryTimeoutMs: 5000,
            },
        }));
    });

    it('should reject a delivery timeout above the poll interval margin', () => {
        assert.throws(() => notificationConfigValidator(null, {
            ...defaultExtConfig,
            deliveryPool: {
                enabled: true,
                topic: 'delivery-topic',
                groupId: 'delivery-group',
                deliveryTimeoutMs: 240001,
            },
        }));
    });
});
