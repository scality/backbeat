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

    it('should default the delivery pool source to the internal topic', () => {
        const config = notificationConfigValidator(null, {
            ...defaultExtConfig,
            deliveryPool: { enabled: true, topic: 't', groupId: 'g' },
        });
        assert.strictEqual(config.deliveryPool.source, 'internal');
    });

    it('should accept the delivery topic as the source and reject others', () => {
        const config = notificationConfigValidator(null, {
            ...defaultExtConfig,
            deliveryPool: {
                enabled: true, topic: 't', groupId: 'g', source: 'delivery',
            },
        });
        assert.strictEqual(config.deliveryPool.source, 'delivery');
        assert.throws(() => notificationConfigValidator(null, {
            ...defaultExtConfig,
            deliveryPool: {
                enabled: true, topic: 't', groupId: 'g', source: 'legacy',
            },
        }));
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

    describe('workgroups', () => {
        const enabledPool = {
            enabled: true,
            topic: 'delivery-topic',
            groupId: 'delivery-group',
        };

        const validate = workgroups => notificationConfigValidator(null, {
            ...defaultExtConfig,
            deliveryPool: { ...enabledPool, workgroups },
        });

        it('should leave the pool without a workgroups block when unset', () => {
            const config = notificationConfigValidator(null, {
                ...defaultExtConfig,
                deliveryPool: enabledPool,
            });
            assert.strictEqual('workgroups' in config.deliveryPool, false);
        });

        it('should fill both path defaults for an empty workgroups block', () => {
            const { workgroups } = validate({}).deliveryPool;
            assert.strictEqual(workgroups.zookeeperPath,
                '/notification/delivery-workgroups');
            assert.strictEqual(workgroups.cachePath,
                '/tmp/backbeat-delivery-workgroups.json');
            assert.strictEqual(workgroups.id, undefined);
            assert.strictEqual(workgroups.generation, undefined);
        });

        it('should reject a workgroup id that fails the pattern', () => {
            ['-wg-bulk-a', 'wg bulk a', 'wg.bulk', ''].forEach(id =>
                assert.throws(() => validate({ id }), `accepted "${id}"`));
        });

        it('should accept a valid workgroup id', () => {
            assert.strictEqual(
                validate({ id: 'wg-bulk-a' }).deliveryPool.workgroups.id,
                'wg-bulk-a');
        });

        it('should reject generation 0 and accept generation 1', () => {
            assert.throws(() => validate({ generation: 0 }));
            assert.strictEqual(
                validate({ generation: 1 }).deliveryPool.workgroups.generation, 1);
        });

        it('should round trip a full workgroups block', () => {
            const workgroups = {
                id: 'wg-whale',
                zookeeperPath: '/notification/delivery-workgroups',
                cachePath: '/var/lib/backbeat/delivery-workgroups.json',
                generation: 3,
            };
            assert.deepStrictEqual(
                validate(workgroups).deliveryPool.workgroups, workgroups);
        });
    });

    describe('kerberos producer selection ::', () => {
        const kerberosAuth = {
            type: 'kerberos',
            protocol: 'SASL_PLAINTEXT',
            keytab: 'notifications.keytab',
            principal: 'notifications@EXAMPLE.COM',
            serviceName: 'kafka',
        };

        const withPool = deliveryPool => notificationConfigValidator(null, {
            ...defaultExtConfig,
            deliveryPool: {
                enabled: true, topic: 'delivery', groupId: 'delivery-group',
                ...deliveryPool,
            },
        });

        const withAuth = auth => notificationConfigValidator(null, {
            ...defaultExtConfig,
            destinations: [{
                resource: 'dest', type: 'kafka', host: 'host', topic: 'topic', auth,
            }],
        }).destinations[0].auth;

        it('should keep node-rdkafka as the default kerberos producer', () => {
            assert.strictEqual(withPool({}).deliveryPool.kerberosProducer, 'rdkafka');
        });

        it('should accept the pure JS kerberos producer', () => {
            assert.strictEqual(
                withPool({ kerberosProducer: 'kafkajs' }).deliveryPool.kerberosProducer,
                'kafkajs');
        });

        it('should accept the platformatic producer for kerberos destinations', () => {
            assert.strictEqual(
                withPool({ kerberosProducer: 'platformatic' }).deliveryPool.kerberosProducer,
                'platformatic');
        });

        it('should reject an unknown kerberos producer', () => {
            assert.throws(() => withPool({ kerberosProducer: 'sarama' }));
        });

        it('should default a kerberos destination to the keytab credential source', () => {
            assert.strictEqual(withAuth(kerberosAuth).credentialSource, 'keytab');
        });

        it('should accept the ccache credential source', () => {
            assert.strictEqual(
                withAuth({ ...kerberosAuth, credentialSource: 'ccache' }).credentialSource,
                'ccache');
        });

        it('should reject an unknown credential source', () => {
            assert.throws(() => withAuth({ ...kerberosAuth, credentialSource: 'vault' }));
        });

        it('should not add a credential source to a non kerberos destination', () => {
            const auth = withAuth({ type: 'basic', protocol: 'SASL_PLAINTEXT',
                username: 'u', password: 'p' });
            assert.strictEqual('credentialSource' in auth, false);
        });
    });
});
