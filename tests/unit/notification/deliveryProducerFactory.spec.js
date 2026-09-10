const assert = require('assert');
const { execFileSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');
const sinon = require('sinon');

const {
    createDeliveryProducer,
    usesKerberosProducer,
} = require('../../../extensions/notification/destination/deliveryProducerFactory');
const DeliveryKafkaProducer = require(
    '../../../extensions/notification/deliveryWorker/DeliveryKafkaProducer');
const KerberosKafkaProducer = require(
    '../../../extensions/notification/destination/KerberosKafkaProducer');

const kerberosAuth = {
    type: 'kerberos',
    protocol: 'SASL_PLAINTEXT',
    keytab: 'notifications.keytab',
    principal: 'notifications@EXAMPLE.COM',
    serviceName: 'kafka',
    credentialSource: 'ccache',
};

const producerConfig = {
    kafka: { hosts: 'broker:9093' },
    topic: 'notifications',
    requiredAcks: 1,
    compressionType: 'none',
    deliveryTimeoutMs: 30000,
};

describe('notification deliveryProducerFactory', () => {
    let confDir;

    beforeEach(() => {
        // the node-rdkafka path resolves the keytab through CONF_DIR, so the
        // file has to exist for a kerberos destination to build at all
        confDir = fs.mkdtempSync(path.join(os.tmpdir(), 'krbfactory-'));
        fs.mkdirSync(path.join(confDir, 'ssl'));
        fs.writeFileSync(path.join(confDir, 'ssl', 'notifications.keytab'),
            Buffer.from([5, 2]));
        process.env.CONF_DIR = confDir;
        sinon.stub(DeliveryKafkaProducer.prototype, 'connect');
        sinon.stub(KerberosKafkaProducer.prototype, '_connect');
    });

    afterEach(() => {
        delete process.env.CONF_DIR;
        fs.rmSync(confDir, { recursive: true, force: true });
        sinon.restore();
    });

    const build = (auth, kerberosProducer) => createDeliveryProducer({
        destConfig: { resource: 'dest', type: 'kafka', host: 'broker', auth },
        producerConfig: { ...producerConfig, auth },
        kerberosProducer,
    });

    describe('selection', () => {
        it('should serve a kerberos destination with node-rdkafka by default', () => {
            assert.strictEqual(build(kerberosAuth, undefined) instanceof DeliveryKafkaProducer,
                true);
        });

        it('should serve a kerberos destination with the pure JS producer when asked', () => {
            assert.strictEqual(build(kerberosAuth, 'kafkajs') instanceof KerberosKafkaProducer,
                true);
        });

        it('should keep node-rdkafka for a non kerberos destination either way', () => {
            ['rdkafka', 'kafkajs'].forEach(stack => {
                const producer = build({ type: 'basic', protocol: 'SASL_PLAINTEXT',
                    username: 'u', password: 'p' }, stack);
                assert.strictEqual(producer instanceof DeliveryKafkaProducer, true,
                    `switched stacks for a basic auth destination on ${stack}`);
            });
        });

        it('should keep node-rdkafka for a destination with no auth at all', () => {
            const producer = createDeliveryProducer({
                destConfig: { resource: 'dest', type: 'kafka', host: 'broker' },
                producerConfig,
                kerberosProducer: 'kafkajs',
            });
            assert.strictEqual(producer instanceof DeliveryKafkaProducer, true);
        });
    });

    describe('predicate', () => {
        it('should answer for every combination it is asked about', () => {
            assert.strictEqual(usesKerberosProducer({ auth: kerberosAuth }, 'kafkajs'), true);
            assert.strictEqual(usesKerberosProducer({ auth: kerberosAuth }, 'rdkafka'), false);
            assert.strictEqual(usesKerberosProducer({ auth: { ssl: true } }, 'kafkajs'), false);
            assert.strictEqual(usesKerberosProducer({}, 'kafkajs'), false);
            assert.strictEqual(usesKerberosProducer(undefined, 'kafkajs'), false);
        });
    });

    describe('native module loading', () => {
        it('should not load the gssapi binding when the flag is off', () => {
            // a deployment that never opts in must not need the native module
            // present, so the require has to stay behind the branch
            const repoRoot = path.resolve(__dirname, '../../..');
            const script = `
                const f = require('${repoRoot}/extensions/notification/destination/` +
                `deliveryProducerFactory');
                const loaded = Object.keys(require.cache)
                    .some(p => p.includes('node_modules/kerberos/'));
                console.log(loaded ? 'LOADED' : 'NOT_LOADED');
            `;
            const out = execFileSync(process.execPath, ['-e', script], { encoding: 'utf8' });
            assert.strictEqual(out.trim(), 'NOT_LOADED');
        });
    });
});
