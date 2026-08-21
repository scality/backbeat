'use strict';

const assert = require('assert');
const joi = require('joi');

const { extensionConfigValidator } = require('../../../../lib/config/extensionConfigValidator');
const { logJoiOptional } = require('../../../../lib/config/configItems.joi');
const extensions = require('../../../../extensions');
const backbeatConfig = require('./config.json');

// the validated global configuration, as Config passes it to an extension
const globalConfig = { log: { logLevel: 'info', dumpLevel: 'trace' } };

function withEnv(env, validate) {
    const og = Object.fromEntries(Object.keys(env).map(name => [name, process.env[name]]));
    Object.assign(process.env, env);
    try {
        return validate();
    } finally {
        Object.entries(og).forEach(([name, value]) => {
            if (value === undefined) {
                delete process.env[name];
            } else {
                process.env[name] = value;
            }
        });
    }
}

describe('extension config validator', () => {
    const schema = joi.object({
        topic: joi.string().required(),
        consumer: joi.object({
            groupId: joi.string().required(),
            concurrency: joi.number().default(10),
        }),
        log: logJoiOptional,
    });
    const validator = extensionConfigValidator('demo', schema);
    // the overrides are applied in place: each case validates its own copy
    const extConfig = () => ({ topic: 'demo-topic', consumer: { groupId: 'demo-group' } });

    it('should apply an env var named after the extension', () => {
        const validated = withEnv(
            { EXTENSIONS_DEMO_TOPIC: 'from-env' },
            () => validator(globalConfig, extConfig()));

        assert.strictEqual(validated.topic, 'from-env');
    });

    it('should apply an env var of a nested field', () => {
        const validated = withEnv({ EXTENSIONS_DEMO_CONSUMER_CONCURRENCY: '42' },
                                  () => validator(globalConfig, extConfig()));

        assert.strictEqual(validated.consumer.concurrency, 42);
    });

    it('should apply the defaults of the schema', () => {
        assert.strictEqual(validator(globalConfig, extConfig()).consumer.concurrency, 10);
    });

    // the global configuration is the validation context, so that a field can
    // default to a global one
    it('should inherit a level from the global log config', () => {
        const validated = withEnv({ EXTENSIONS_DEMO_LOG_LEVEL: 'debug' },
                                  () => validator(globalConfig, extConfig()));

        assert.deepStrictEqual(validated.log, { logLevel: 'debug', dumpLevel: 'trace' });
    });

    it('should reject a value the schema does not accept', () => {
        assert.throws(() => withEnv({ EXTENSIONS_DEMO_CONSUMER_CONCURRENCY: 'many' },
                                    () => validator(globalConfig, extConfig())),
                      /"consumer.concurrency" must be a number/);
    });

    it('should reject a field the schema does not declare', () => {
        assert.throws(() => validator(globalConfig, { ...extConfig(), unknown: 1 }),
                      /"unknown" is not allowed/);
    });

    /**
     * Every extension is validated through the same factory: the env var of one
     * of its fields is checked here, so that a schema losing the annotations,
     * or an extension added without them, is caught.
     */
    describe('extensions', () => {
        const cases = [
            ['gc', 'EXTENSIONS_GC_TOPIC', 'topic'],
            ['ingestion', 'EXTENSIONS_INGESTION_TOPIC', 'topic'],
            ['lifecycle', 'EXTENSIONS_LIFECYCLE_ZOOKEEPER_PATH', 'zookeeperPath'],
            ['mongoProcessor', 'EXTENSIONS_MONGO_PROCESSOR_TOPIC', 'topic'],
            ['notification', 'EXTENSIONS_NOTIFICATION_TOPIC', 'topic'],
            ['oplogPopulator', 'EXTENSIONS_OPLOG_POPULATOR_TOPIC', 'topic'],
            ['replication', 'EXTENSIONS_REPLICATION_TOPIC', 'topic'],
        ];

        cases.forEach(([name, envVar, field]) => {
            it(`should apply ${envVar}`, () => {
                const config = JSON.parse(JSON.stringify(backbeatConfig.extensions[name]));
                const validated = withEnv(
                    { [envVar]: 'from-env' },
                    () => extensions[name].configValidator(globalConfig, config));

                assert.strictEqual(validated[field], 'from-env');
            });
        });

        it('should account for every extension holding a config validator', () => {
            const covered = new Set(cases.map(([name]) => name));
            const validated = Object.entries(extensions)
                .filter(([, extension]) => extension.configValidator)
                .map(([name]) => name);

            assert.deepStrictEqual(validated.filter(name => !covered.has(name)), []);
        });
    });
});
