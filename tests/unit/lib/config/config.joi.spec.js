'use strict';

const assert = require('assert');
const joi = require('joi');

const { backbeatConfigJoi } = require('../../../../lib/config.joi');
const { envVarMappings } = require('../../../../lib/config/envOverrides');

describe('backbeat config schema', () => {
    it('should derive no name for the params BackbeatProducer sets itself', () => {
        const names = [...envVarMappings(backbeatConfigJoi).keys()];

        assert.ok(!names.some(name => name.startsWith('KAFKA_PRODUCER_PARAMS')), names.join(', '));
    });

    describe('queuePopulator', () => {
        // the probe server of the populator is conditioned by `...extensions`:
        // the section is validated under a parent holding one
        const schema = joi.object({
            queuePopulator: backbeatConfigJoi.extract('queuePopulator'),
            extensions: joi.object(),
        });
        const base = {
            auth: { type: 'none', vault: { host: 'vault', port: 8500 } },
            cronRule: '* * * * *',
            zookeeperPath: '/backbeat',
            probeServer: { port: 8550 },
        };
        const validate = queuePopulator =>
            schema.validate({ queuePopulator: { ...base, ...queuePopulator }, extensions: { gc: {} } });

        // the log source names the section the populator reads the oplog from
        it('should require the section of its log source', () => {
            assert.match(validate({ logSource: 'bucketd' }).error.message, /"queuePopulator.bucketd" is required/);
            assert.match(validate({ logSource: 'dmd' }).error.message, /"queuePopulator.dmd" is required/);
            assert.match(validate({ logSource: 'kafka' }).error.message, /"queuePopulator.kafka" is required/);
        });

        it('should accept the log source its section configures', () => {
            assert.strictEqual(validate({ logSource: 'dmd', dmd: { host: 'dmd', port: 9990 } }).error,
                               undefined);
        });

        // the ingestion reader is configured by the extension
        it('should need no section for the ingestion log source', () => {
            assert.strictEqual(validate({ logSource: 'ingestion' }).error, undefined);
        });

        it('should reject a log source it cannot read', () => {
            assert.match(validate({ logSource: 'mongo' }).error.message,
                         /"queuePopulator.logSource" must be one of \[bucketd, ingestion, dmd, kafka\]/);
        });
    });

    describe('redis', () => {
        const redisJoi = backbeatConfigJoi.extract('redis');
        const validate = redis => joi.attempt(redis, redisJoi);

        // a configuration that does not mention redis reaches it locally, as
        // the standalone section of the shipped configuration file does
        it('should default to a local standalone server', () => {
            assert.deepStrictEqual(joi.attempt({}, joi.object({ redis: redisJoi })),
                                   { redis: { host: '127.0.0.1', port: 6379 } });
        });

        describe('sentinels', () => {
            it('should default the group name of the master they watch', () => {
                assert.deepStrictEqual(validate({ sentinels: 'host1:26379' }), {
                    sentinels: [{ host: 'host1', port: 26379 }],
                    name: 'mymaster',
                    password: '',
                    sentinelPassword: '',
                });
            });

            it('should keep the configured group name', () => {
                assert.strictEqual(validate({ sentinels: 'host1:26379', name: 'group' }).name, 'group');
            });

            // the redis client expects a list: a comma separated one is parsed
            it('should parse the comma separated form', () => {
                assert.deepStrictEqual(
                    validate({ sentinels: 'host1:26379,host2:26380' }).sentinels,
                    [{ host: 'host1', port: 26379 }, { host: 'host2', port: 26380 }]);
            });

            it('should keep the passwords of a comma separated form', () => {
                const redis = { sentinels: 'host1:26379', password: 'p', sentinelPassword: 's' };

                assert.deepStrictEqual(validate(redis), {
                    sentinels: [{ host: 'host1', port: 26379 }],
                    name: 'mymaster',
                    password: 'p',
                    sentinelPassword: 's',
                });
            });

            it('should accept a list of host and port', () => {
                const sentinels = [{ host: 'host1', port: 26379 }, { host: 'host2', port: 26379 }];

                assert.deepStrictEqual(validate({ sentinels }).sentinels, sentinels);
            });

            // the two modes are exclusive: a deployment configures one of them
            it('should reject the standalone host and port', () => {
                assert.throws(() => validate({ sentinels: 'host1:26379', host: 'redis' }),
                              /"host" is not allowed/);
                assert.throws(() => validate({ sentinels: 'host1:26379', port: 6380 }),
                              /"port" is not allowed/);
            });
        });

        describe('standalone', () => {
            it('should default the port', () => {
                assert.deepStrictEqual(validate({ host: 'redis' }),
                                       { host: 'redis', port: 6379, password: '' });
            });

            it('should keep the configured port', () => {
                assert.strictEqual(validate({ host: 'redis', port: '6380' }).port, 6380);
            });

            it('should require a host', () => {
                assert.throws(() => validate({}), /"host" is required/);
                assert.throws(() => validate({ port: 6380 }), /"host" is required/);
            });

            // the group name and the sentinel password only mean something to
            // the sentinels, and are not part of this mode
            it('should reject the settings of the sentinels', () => {
                assert.throws(() => validate({ host: 'redis', name: 'group' }),
                              /"name" is not allowed/);
                assert.throws(() => validate({ host: 'redis', sentinelPassword: 'p' }),
                              /"sentinelPassword" is not allowed/);
            });
        });
    });
});
