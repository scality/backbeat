const assert = require('assert');

const configValidator = require('../../../extensions/replication/ReplicationConfigValidator');
const { replication } = require('../../config.json').extensions;

const baseConfig = {
    source: {
        transport: 'http',
        s3: {
            host: '127.0.0.1',
            port: 8000
        },
        auth: {
            type: 'service',
            account: 'service-replication',
            vault: {
                host: '127.0.0.1',
                port: 8500,
                adminPort: 8600
            }
        }
    },
    destination: {
        transport: 'http',
        bootstrapList: [
            { site: 'aws1', type: 'aws_s3' },
            { site: 'aws2', type: 'aws_s3' },
            { site: 'aws3', type: 'aws_s3' },
        ],
        auth: {
            type: 'service',
            account: 'service-replication'
        }
    },
    topic: 'backbeat-replication',
    dataMoverTopic: 'backbeat-data-mover',
    replicationStatusTopic: 'backbeat-replication-status',
    replicationFailedTopic: 'backbeat-replication-failed',
    monitorReplicationFailures: true,
    monitorReplicationFailureExpiryTimeS: 86400,
    replayTopics: [
        {
            topicName: 'backbeat-replication-replay-0',
            retries: 5
        }
    ],
    queueProcessor: {
        groupId: 'backbeat-replication-group',
        retry: {
            scality: {
                maxRetries: 5,
                timeoutS: 300,
                backoff: {
                    min: 1000,
                    max: 300000,
                    jitter: 0.1,
                    factor: 1.5
                }
            }
        },
        concurrency: 10,
        mpuPartsConcurrency: 10,
        probeServer: {
            bindAddress: 'localhost',
            port: 4043
        }
    },
    replicationStatusProcessor: {
        groupId: 'backbeat-replication-group',
        retry: {
            maxRetries: 5,
            timeoutS: 300,
            backoff: {
                min: 1000,
                max: 300000,
                jitter: 0.1,
                factor: 1.5
            }
        },
        concurrency: 10,
        probeServer: {
            bindAddress: 'localhost',
            port: 4045
        }
    },
    objectSizeMetrics: [
        66560,
        8388608,
        68157440
    ]
};

describe('ReplicationConfigValidator', () => {
    it('should require all sites to have a config when destination auth is per site', () => {
        const config = {
            ...baseConfig,
            destination: {
                ...baseConfig.destination,
                sites: {
                    aws1: {
                        auth: { type: 'service', account: 'service-replication' },
                    },
                    aws4: {
                        auth: { type: 'service', account: 'service-replication' },
                    },
                }
            }
        };
        delete config.destination.auth;
        assert.throws(() => configValidator({}, config),
            err => err.message === 'missing destination configuration for sites: aws2,aws3');
    });

    it('should not require all sites to have a config when destination.auth is defined', () => {
        const config = {
            ...baseConfig,
            destination: {
                ...baseConfig.destination,
                sites: {
                    aws1: {
                        auth: { type: 'service', account: 'service-replication' },
                    },
                    aws4: {
                        auth: { type: 'service', account: 'service-replication' },
                    },
                }
            }
        };
        assert.doesNotThrow(() => configValidator({}, config));
    });

    it('should allow specifying a custom transport for a site', () => {
        const config = {
            ...baseConfig,
            destination: {
                ...baseConfig.destination,
                sites: {
                    aws1: {
                        transport: 'https',
                    },
                }
            }
        };
        assert.doesNotThrow(() => configValidator({}, config));
    });

    it('should require destination auth to contain sts config when type is assumeRole', () => {
        const config = {
            ...baseConfig,
            destination: {
                ...baseConfig.destination,
                sites: {
                    aws1: {
                        auth: {
                            type: 'assumeRole',
                            account: 'service-replication'
                        },
                    },
                    aws2: {
                        auth: {
                            type: 'service',
                            account: 'service-replication'
                        },
                    },
                    aws3: {
                        auth: {
                            type: 'service',
                            account: 'service-replication'
                        },
                    },
                },
            }
        };
        assert.throws(() => configValidator({}, config),
            err => err.message === '"destination.sites.aws1.auth.sts" is required');
    });

    it('should validate new destination schema', () => {
        const config = {
            ...baseConfig,
            destination: {
                ...baseConfig.destination,
                sites: {
                    aws1: {
                        transport: 'http',
                        auth: {
                            type: 'assumeRole',
                            sts: {
                                host: 'sts.enpoint.com',
                                port: 80,
                                accessKey: 'accessKey',
                                secretKey: 'secretKey',
                            },
                        },
                    },
                    aws2: {
                        transport: 'http',
                        auth: {
                            type: 'service',
                            account: 'service-replication',
                        },
                    },
                    aws3: {
                        transport: 'http',
                        auth: {
                            type: 'service',
                            account: 'service-replication',
                        },
                    },
                },
            }
        };
        delete config.destination.auth;
        delete config.destination.transport;
        assert.doesNotThrow(() => configValidator({}, config));
    });

    [
        { transport: 'http', port: undefined, expected: 80 },
        { transport: 'https', port: null, expected: 443 },
        { transport: 'https', port: '', expected: 443 },
        { transport: 'https', port: 7841, expected: 7841 },
        { transport: 'https', port: '3426', expected: 3426 },
    ].forEach(({ transport, port, expected }) =>
        it(`should use default sts port when ${transport} port is ${port === '' ? '""' : port}`, () => {
            const config = {
                ...baseConfig,
                destination: {
                    ...baseConfig.destination,
                    bootstrapList: [
                        { site: 'aws', type: 'aws_s3' },
                    ],
                    sites: {
                        aws: {
                            transport,
                            auth: {
                                type: 'assumeRole',
                                sts: {
                                    host: 'sts.enpoint.com',
                                    port,
                                    accessKey: 'accessKey',
                                    secretKey: 'secretKey',
                                },
                            },
                        },
                    },
                },
            };
            delete config.destination.auth;
            delete config.destination.transport;

            const result = configValidator({}, config);
            assert.strictEqual(result.destination.sites.aws.auth.sts.port, expected);
        })
    );

    it('should validate old destination schema', () => {
        const config = {
            ...baseConfig,
            destination: {
                ...baseConfig.destination,
                transport: undefined,
                auth: {
                    type: 'service',
                    account: 'service-replication'
                }
            }
        };
        assert.doesNotThrow(() => configValidator({}, config));
    });

    it('should load admin credentials from file when adminCredentialsFile is set', () => {
        const config = {
            ...baseConfig,
            source: {
                ...baseConfig.source,
                auth: {
                    type: 'role',
                    vault: {
                        host: 'localhost',
                        port: 8200,
                        adminPort: 8201,
                        adminCredentialsFile: `${__dirname}/utils/admin-backbeat.json`,
                    },
                },
            },
            destination: {
                ...baseConfig.destination,
                auth: {
                    type: 'role',
                    vault: {
                        host: 'localhost',
                        port: 8200,
                        adminPort: 8201,
                        adminCredentialsFile: `${__dirname}/utils/admin-backbeat.json`,
                    },
                },
                sites: {
                    aws1: {
                        auth: {
                            type: 'role',
                            vault: {
                                host: 'localhost',
                                port: 8200,
                                adminPort: 8201,
                                adminCredentialsFile: `${__dirname}/utils/admin-backbeat.json`,
                            },
                        },
                    },
                },
            }
        };
        const conf = configValidator({}, config);
        const adminCredentials = {
            accessKey: 'HPZYD8TCLETBDZBPNTNQ',
            secretKey: 'prMKjH3Epf2O7+pLA0iwW9OcZjiFVRpHT=7ir3w6',
        };
        assert.deepStrictEqual(
            conf.source.auth.vault.adminCredentials,
            adminCredentials,
        );
        assert.deepStrictEqual(
            conf.destination.auth.vault.adminCredentials,
            adminCredentials,
        );
        assert.deepStrictEqual(
            conf.destination.auth.vault.adminCredentials,
            adminCredentials,
        );
        assert.deepStrictEqual(
            conf.destination.sites.aws1.auth.vault.adminCredentials,
            adminCredentials,
        );
    });
});

// tests/config.json sets a distinctive (non-default) value so a pass-through is
// provable: queueProcessor 350000.

function withoutMaxPollInterval() {
    const clone = JSON.parse(JSON.stringify(replication));
    delete clone.queueProcessor.maxPollIntervalMs;
    return clone;
}

function withValue(maxPollIntervalMs) {
    return {
        ...replication,
        queueProcessor: {
            ...replication.queueProcessor,
            maxPollIntervalMs,
        },
    };
}

describe('ReplicationConfigValidator maxPollIntervalMs', () => {
    it('should read the queueProcessor value from config', () => {
        const validated = configValidator(null, replication);
        assert.strictEqual(
            validated.queueProcessor.maxPollIntervalMs, 350000);
    });

    it('should leave it unset when not configured', () => {
        const validated = configValidator(null, withoutMaxPollInterval());
        assert.strictEqual(
            validated.queueProcessor.maxPollIntervalMs, undefined);
    });

    it('should reject a value below 45000', () => {
        assert.throws(
            () => configValidator(null, withValue(30000)),
            /greater than or equal to 45000/);
    });

    it('should reject a value above 1800000 (30 minutes)', () => {
        assert.throws(
            () => configValidator(null, withValue(1900000)),
            /less than or equal to 1800000/);
    });
});
