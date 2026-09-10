const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const sinon = require('sinon');

const KerberosKafkaProducer = require(
    '../../../extensions/notification/destination/KerberosKafkaProducer');
const { processCredentials } = require(
    '../../../extensions/notification/destination/kerberosCredentials');

const kerberosAuth = {
    type: 'kerberos',
    protocol: 'SASL_PLAINTEXT',
    keytab: 'notifications.keytab',
    principal: 'notifications@EXAMPLE.COM',
    serviceName: 'kafka',
    credentialSource: 'ccache',
};

const baseConfig = {
    kafka: { hosts: 'broker-1:9093' },
    topic: 'notifications',
    auth: kerberosAuth,
    requiredAcks: 1,
    compressionType: 'none',
    deliveryTimeoutMs: 20000,
};

/**
 * Build a producer whose connect never runs, with a fake kafkajs producer
 * attached, so the surface can be exercised without a broker
 * @param {Object} [overrides] - config overrides
 * @return {Object} { producer, sent, disconnects }
 */
function makeProducer(overrides) {
    const producer = new KerberosKafkaProducer({ ...baseConfig, ...overrides });
    const sent = [];
    const disconnects = [];
    producer._producer = {
        send: async payload => {
            sent.push(payload);
            return [{ topicName: payload.topic, partition: 0, baseOffset: '7' }];
        },
        disconnect: async () => {
            disconnects.push(true);
        },
    };
    producer._ready = true;
    return { producer, sent, disconnects };
}

describe('notification KerberosKafkaProducer', () => {
    let connectStub;

    beforeEach(() => {
        connectStub = sinon.stub(KerberosKafkaProducer.prototype, '_connect');
    });

    afterEach(() => sinon.restore());

    describe('configuration', () => {
        it('should split a broker list and apply the topic prefix', () => {
            const { producer } = makeProducer({ kafka: { hosts: 'b1:9093, b2:9093' } });
            assert.deepStrictEqual(producer._brokers, ['b1:9093', 'b2:9093']);
            assert.strictEqual(producer._topic, 'notifications');
        });

        it('should refuse an auth configuration that is not kerberos', () => {
            assert.throws(
                () => new KerberosKafkaProducer({ ...baseConfig, auth: { ssl: true } }),
                /needs an auth configuration of type kerberos/);
        });

        it('should accept gzip, which kafkajs compresses in process', () => {
            ['gzip', 'Gzip', 'GZIP'].forEach(compressionType => {
                const { producer } = makeProducer({ compressionType });
                assert.notStrictEqual(producer._compression, undefined,
                    `rejected ${compressionType}`);
            });
        });

        it('should default to no compression, as a kafka destination does', () => {
            const { producer } = makeProducer({ compressionType: undefined });
            assert.strictEqual(producer._compression, 0);
        });

        it('should refuse a codec kafkajs cannot compress on its own', () => {
            assert.throws(
                () => new KerberosKafkaProducer({ ...baseConfig, compressionType: 'Zstd' }),
                /unsupported compressionType "Zstd"/);
        });

        it('should reject a config with no broker list', () => {
            assert.throws(() => new KerberosKafkaProducer({ ...baseConfig, kafka: {} }));
        });

        it('should take the poll interval without using it', () => {
            // the pool builds either producer from one config, and kafkajs
            // has no poll loop to schedule
            assert.doesNotThrow(() => makeProducer({ pollIntervalMs: 2000 }));
        });
    });

    describe('send', () => {
        it('should publish entries to the configured topic', done => {
            const { producer, sent } = makeProducer();
            producer.send([
                { key: 'k1', message: '{"a":1}' },
                { key: 'k2', message: '{"a":2}' },
            ], (err, reports) => {
                assert.ifError(err);
                assert.strictEqual(sent.length, 1);
                assert.strictEqual(sent[0].topic, 'notifications');
                assert.deepStrictEqual(sent[0].messages.map(m => m.key), ['k1', 'k2']);
                assert.strictEqual(sent[0].messages[0].value.toString(), '{"a":1}');
                assert.strictEqual(reports.length, 1);
                done();
            });
        });

        it('should carry the required acks and the delivery timeout', done => {
            const { producer, sent } = makeProducer({ requiredAcks: -1 });
            producer.send([{ key: 'k', message: 'm' }], err => {
                assert.ifError(err);
                assert.strictEqual(sent[0].acks, -1);
                assert.strictEqual(sent[0].timeout, 20000);
                done();
            });
        });

        it('should call back without publishing when there is nothing to send', done => {
            const { producer, sent } = makeProducer();
            producer.send([], err => {
                assert.ifError(err);
                assert.strictEqual(sent.length, 0);
                done();
            });
        });

        it('should refuse to send before the producer is ready', done => {
            const { producer } = makeProducer();
            producer._ready = false;
            producer.send([{ key: 'k', message: 'm' }], err => {
                assert.strictEqual(err.is.InternalError, true);
                done();
            });
        });

        it('should refuse to send with no topic configured', done => {
            const { producer } = makeProducer({ topic: undefined });
            producer.send([{ key: 'k', message: 'm' }], err => {
                assert.strictEqual(err.is.InternalError, true);
                done();
            });
        });

        it('should report a publish failure to the delivery callback', done => {
            const { producer } = makeProducer();
            producer._producer.send = async () => {
                throw new Error('Not authorized to access topics');
            };
            producer.send([{ key: 'k', message: 'm' }], err => {
                assert.strictEqual(err.is.InternalError, true);
                assert.match(err.description, /Not authorized to access topics/);
                done();
            });
        });
    });

    describe('close', () => {
        it('should disconnect the client and stop accepting sends', done => {
            const { producer, disconnects } = makeProducer();
            producer.close(err => {
                assert.ifError(err);
                assert.strictEqual(disconnects.length, 1);
                assert.strictEqual(producer._ready, false);
                done();
            });
        });

        it('should count a producer that never connected as closed', done => {
            const { producer } = makeProducer();
            producer._producer = null;
            producer.close(err => {
                assert.ifError(err);
                done();
            });
        });

        it('should hand a disconnect failure to the caller', done => {
            const { producer } = makeProducer();
            producer._producer.disconnect = async () => {
                throw new Error('flush timed out');
            };
            producer.close(err => {
                assert.match(err.message, /flush timed out/);
                done();
            });
        });
    });

    describe('credentials', () => {
        let confDir;

        beforeEach(() => {
            confDir = fs.mkdtempSync(path.join(os.tmpdir(), 'krbproducer-'));
            fs.mkdirSync(path.join(confDir, 'ssl'));
            process.env.CONF_DIR = confDir;
        });

        afterEach(() => {
            delete process.env.CONF_DIR;
            fs.rmSync(confDir, { recursive: true, force: true });
        });

        it('should leave the credential cache alone for the ccache source', () => {
            const register = sinon.stub(processCredentials, 'registerKeytab');
            const { producer } = makeProducer();
            producer._prepareCredentials();
            assert.strictEqual(register.called, false);
        });

        it('should register the destination keytab for the keytab source', () => {
            const keytabPath = path.join(confDir, 'ssl', 'notifications.keytab');
            fs.writeFileSync(keytabPath, Buffer.from([5, 2]));
            const register = sinon.stub(processCredentials, 'registerKeytab');
            const { producer } = makeProducer({
                auth: { ...kerberosAuth, credentialSource: 'keytab' },
            });
            producer._prepareCredentials();
            assert.strictEqual(register.firstCall.args[0], keytabPath);
        });

        it('should name the keytab it could not find', () => {
            const { producer } = makeProducer({
                auth: { ...kerberosAuth, credentialSource: 'keytab' },
            });
            assert.throws(() => producer._prepareCredentials(),
                /Keytab file notifications.keytab not found/);
        });

        it('should report a credential problem as an error event, not a throw', done => {
            // the pool and the destination both learn about an unusable
            // producer from the error event, so the constructor must not throw
            connectStub.restore();
            const producer = new KerberosKafkaProducer({
                ...baseConfig,
                auth: { ...kerberosAuth, credentialSource: 'keytab' },
            });
            producer.on('error', err => {
                assert.match(err.message, /Keytab file notifications.keytab not found/);
                done();
            });
        });
    });
});
