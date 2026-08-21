'use strict';

const assert = require('assert');
const joi = require('joi');

const { backbeatConfigJoi } = require('../../../../lib/config.joi');

describe('backbeat config schema', () => {
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
