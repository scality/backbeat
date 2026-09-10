/*
 * One delivery pool serving ONE kerberised destination, run as its own
 * process. The two process control for the kerberos suite: it is the shape
 * that already works today, so it has to pass for the suite's failures to
 * mean anything.
 *
 * argv[2] is 'a' or 'b'. The result is sent back to the parent over the
 * child process channel.
 */
const async = require('async');
const werelogs = require('werelogs');

const DeliveryProducerPool = require(
    '../../../extensions/notification/deliveryWorker/DeliveryProducerPool');

const NAME = process.argv[2];
const REALM = process.env.KRB_REALM || 'SCALITY.TEST';
const BROKERS = process.env.KRB_BROKERS || 'localhost:19095';
const ROUNDS = Number(process.env.KRB_ROUNDS || 3);
const ROUND_MS = Number(process.env.KRB_ROUND_MS || 2000);

werelogs.configure({ level: 'info', dump: 'error' });
const log = new werelogs.Logger(`KerberosSingleDestination-${NAME}`);

const [host, port] = BROKERS.split(':');
const destinationId = `krb-single-${NAME}`;
const pool = new DeliveryProducerPool({
    destinationsById: {
        [destinationId]: {
            resource: destinationId,
            type: 'kafka',
            host,
            port: Number(port),
            topic: `topic-${NAME}`,
            requiredAcks: 1,
            compressionType: 'none',
            auth: {
                type: 'kerberos',
                protocol: 'SASL_PLAINTEXT',
                keytab: `notif${NAME}.keytab`,
                principal: `notif${NAME}@${REALM}`,
                serviceName: process.env.KRB_SERVICE || 'kafka',
                credentialSource: 'keytab',
            },
        },
    },
    deliveryPoolConfig: {
        deliveryTimeoutMs: 30000,
        producerIdleMs: 300000,
        maxProducers: 5,
        kerberosProducer: 'kafkajs',
    },
    logger: log,
});

const result = { name: NAME, sent: 0, delivered: 0, errors: [] };

async.timesSeries(ROUNDS, (index, next) => {
    result.sent++;
    pool.get(destinationId, (err, producer) => {
        if (err) {
            result.errors.push(`get:${err.message}`);
            return setTimeout(next, ROUND_MS);
        }
        const message = JSON.stringify({
            arm: 'D', producer: NAME, round: index + 1,
            runId: process.env.KRB_RUN_ID || 'single',
        });
        return producer.send([{ key: `D-${NAME}-${index + 1}`, message }], sendErr => {
            if (sendErr) {
                result.errors.push(`send:${sendErr.description || sendErr.message}`);
            } else {
                result.delivered++;
            }
            setTimeout(next, ROUND_MS);
        });
    });
}, () => {
    log.info('single destination run finished', result);
    if (process.send) {
        process.send(result);
    }
    pool.closeAll(() => process.exit(result.errors.length === 0 ? 0 : 1));
});
