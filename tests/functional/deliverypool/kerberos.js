const assert = require('assert');
const async = require('async');
const { execFile, fork } = require('child_process');
const fs = require('fs');
const path = require('path');
const { KafkaConsumer } = require('node-rdkafka');
const werelogs = require('werelogs');

const DeliveryProducerPool = require(
    '../../../extensions/notification/deliveryWorker/DeliveryProducerPool');
const {
    authenticatedPrincipals,
    containerLogSince,
    execInContainer,
    restartContainer,
    rigControlAvailable,
} = require('./kerberosRig');

/*
 * Two kerberised destinations in ONE delivery worker process.
 *
 * The rig is described in README-kerberos.md. What makes the assertion binary
 * is the broker's ACLs: notifa may only write topic-a and notifb only
 * topic-b, so a producer that authenticated as the wrong principal gets
 * TOPIC_AUTHORIZATION_FAILED rather than quietly delivering to the right
 * topic as the wrong user. The broker's own authenticationID line is the only
 * evidence of the identity; a client stack can report the identity it was
 * configured with.
 */

const REALM = process.env.KRB_REALM || 'SCALITY.TEST';
const BROKERS = process.env.KRB_BROKERS || 'localhost:19095';
const VERIFY_BROKERS = process.env.KRB_VERIFY_BROKERS || 'localhost:19096';
const KAFKA_CONTAINER = process.env.KRB_KAFKA_CONTAINER || 'bnaaskrb-kafka';
const KDC_CONTAINER = process.env.KRB_KDC_CONTAINER || 'bnaaskrb-kdc';
const SERVICE_NAME = process.env.KRB_SERVICE || 'kafka';

const ROUNDS = Number(process.env.KRB_ROUNDS || 9);
const ROUND_MS = Number(process.env.KRB_ROUND_MS || 7000);
const ARM_TIMEOUT = ROUNDS * ROUND_MS + 400000;

// one second of docker log granularity, so an arm's slice never picks up the
// tail of the arm before it
const ARM_GAP_MS = 1500;

const RUN_ID = `${Date.now()}`;

werelogs.configure({ level: 'info', dump: 'error' });
const log = new werelogs.Logger('KerberosDeliveryPoolTest');

/**
 * Count how many connections the broker authenticated per principal
 * @param {string[]} principals - authentication ids in log order
 * @return {Object} principal -> connection count
 */
function countByPrincipal(principals) {
    const counts = {};
    principals.forEach(principal => {
        counts[principal] = (counts[principal] || 0) + 1;
    });
    return counts;
}

/**
 * A destination the pool can serve, keyed by resource name
 * @param {string} name - 'a' or 'b'
 * @param {string} credentialSource - 'keytab' or 'ccache'
 * @return {Object} destination config
 */
function destination(name, credentialSource) {
    const [host, port] = BROKERS.split(':');
    return {
        resource: `krb-dest-${name}`,
        type: 'kafka',
        host,
        port: Number(port),
        topic: `topic-${name}`,
        requiredAcks: 1,
        compressionType: 'none',
        auth: {
            type: 'kerberos',
            protocol: 'SASL_PLAINTEXT',
            keytab: `notif${name}.keytab`,
            principal: `notif${name}@${REALM}`,
            serviceName: SERVICE_NAME,
            credentialSource,
        },
    };
}

/**
 * Build a pool exactly as the delivery worker does
 * @param {Object} params - { kerberosProducer, credentialSource }
 * @return {DeliveryProducerPool} pool under test
 */
function makePool(params) {
    const credentialSource = params.credentialSource || 'keytab';
    return new DeliveryProducerPool({
        destinationsById: {
            'krb-dest-a': destination('a', credentialSource),
            'krb-dest-b': destination('b', credentialSource),
        },
        deliveryPoolConfig: {
            deliveryTimeoutMs: 30000,
            producerIdleMs: 300000,
            maxProducers: 50,
            kerberosProducer: params.kerberosProducer,
        },
        logger: log,
    });
}

/**
 * Interleave sends from both destinations through the pool.
 *
 * Every round asks the pool for a producer, the way the delivery worker does
 * per record, so a producer that lost its identity shows up as a failed
 * delivery rather than as a silent success.
 *
 * @param {DeliveryProducerPool} pool - pool under test
 * @param {Object} params - { arm, rounds, roundMs }
 * @param {function} done - cb(err, results)
 * @return {undefined}
 */
function runRounds(pool, params, done) {
    const arm = params.arm;
    const rounds = params.rounds === undefined ? ROUNDS : params.rounds;
    const roundMs = params.roundMs === undefined ? ROUND_MS : params.roundMs;
    const results = {
        a: { sent: 0, delivered: 0, errors: [] },
        b: { sent: 0, delivered: 0, errors: [] },
    };
    async.timesSeries(rounds, (index, nextRound) => {
        const round = index + 1;
        const startedAt = Date.now();
        async.each(['a', 'b'], (name, nextDest) => {
            const result = results[name];
            result.sent++;
            pool.get(`krb-dest-${name}`, (err, producer) => {
                if (err) {
                    result.errors.push(`get:${err.message}`);
                    return nextDest();
                }
                const message = JSON.stringify({
                    arm, producer: name, round, runId: RUN_ID,
                });
                return producer.send([{ key: `${arm}-${name}-${round}`, message }],
                    sendErr => {
                        if (sendErr) {
                            result.errors.push(
                                `send:${sendErr.description || sendErr.message}`);
                        } else {
                            result.delivered++;
                        }
                        nextDest();
                    });
            });
        }, () => {
            log.info('round done', {
                arm, round, rounds,
                a: `${results.a.delivered}/${results.a.sent}`,
                b: `${results.b.delivered}/${results.b.sent}`,
                rssMB: Math.round(process.memoryUsage().rss / 1048576),
            });
            const rest = roundMs - (Date.now() - startedAt);
            if (round >= rounds || rest <= 0) {
                return setImmediate(nextRound);
            }
            return setTimeout(nextRound, rest);
        });
    }, err => done(err, results));
}

/**
 * Read one topic back over the plaintext listener, where the admin identity
 * is a super user, and count this run's messages per arm
 * @param {string} topic - topic to read
 * @param {function} cb - cb(err, countsByArm)
 * @return {undefined}
 */
function readBack(topic, cb) {
    const consumer = new KafkaConsumer({
        'metadata.broker.list': VERIFY_BROKERS,
        'group.id': `krb-readback-${RUN_ID}-${topic}`,
        'enable.auto.commit': false,
    }, {
        // topic level config: a fresh group with the global setting alone
        // starts at the end of the topic and reads nothing
        'auto.offset.reset': 'earliest',
    });
    const counts = {};
    let finished = false;
    const finish = err => {
        if (finished) {
            return;
        }
        finished = true;
        consumer.disconnect(() => cb(err, counts));
    };
    consumer.on('ready', () => {
        consumer.subscribe([topic]);
        consumer.consume();
        // the topic holds every arm of every run, so consuming to the end and
        // filtering on this run's id is what keeps the counts meaningful
        setTimeout(finish, 15000);
    });
    consumer.on('data', message => {
        let parsed;
        try {
            parsed = JSON.parse(message.value.toString());
        } catch {
            return;
        }
        if (parsed.runId !== RUN_ID) {
            return;
        }
        const key = `${parsed.arm}:${parsed.producer}`;
        counts[key] = (counts[key] || 0) + 1;
    });
    consumer.on('event.error', err => finish(err));
    consumer.connect();
}

/*
 * Arm C is the control on the shipping producer, and it only says something
 * if it is not a race.
 *
 * librdkafka passes GSS_C_NO_CREDENTIAL, so every GSSAPI handshake it makes
 * authenticates as whatever principal the process default credential cache
 * holds; sasl.kerberos.principal only renders the kinit command librdkafka
 * runs per client. Two clients created back to back therefore each overwrite
 * that one cache, and which identity a handshake gets depends on whether it
 * lands before or after the other client's kinit. Populating the cache once
 * and putting a kinit that does nothing ahead of the real one on PATH takes
 * that ordering out of it: the cache holds notifa for the whole arm, both
 * producers authenticate as notifa, and the destination that needs notifb is
 * denied every record it sends.
 */
const ARM_C_CACHE = `/tmp/krb-arm-c-${RUN_ID}.cc`;
const ARM_C_BIN = `/tmp/krb-arm-c-bin-${RUN_ID}`;
const ARM_C_ROUNDS = 3;
const ARM_C_PRINCIPAL = 'a';

// the PATH to put back once the arm is over, so that no later arm, and no
// process arm D forks, inherits the no-op kinit
let pathBeforeArmC = null;

/**
 * Put a kinit that does nothing ahead of the real one on PATH.
 *
 * librdkafka runs its kinit command through system(), which resolves it
 * against the process environment, and node writes process.env through
 * setenv, so this reaches the C library.
 *
 * @return {undefined}
 */
function installNoopKinit() {
    fs.mkdirSync(ARM_C_BIN, { recursive: true });
    const shim = path.join(ARM_C_BIN, 'kinit');
    fs.writeFileSync(shim, '#!/bin/sh\nexit 0\n');
    fs.chmodSync(shim, 0o755);
    pathBeforeArmC = process.env.PATH;
    process.env.PATH = `${ARM_C_BIN}:${process.env.PATH}`;
}

/**
 * Undo installNoopKinit and the credential cache it was installed for
 * @return {undefined}
 */
function restoreArmCEnvironment() {
    if (pathBeforeArmC === null) {
        return;
    }
    process.env.PATH = pathBeforeArmC;
    pathBeforeArmC = null;
    delete process.env.KRB5CCNAME;
}

/**
 * Populate a DIR: credential cache collection with a ticket per principal,
 * standing in for whatever a deployment uses to do that out of band
 * @param {string} collectionDir - directory of the collection
 * @param {function} cb - cb(err)
 * @return {undefined}
 */
function kinitCollection(collectionDir, cb) {
    process.env.KRB5CCNAME = `DIR:${collectionDir}`;
    async.eachSeries(['a', 'b'], (name, next) => {
        execFile('kinit', [
            '-k', '-t', `${process.env.CONF_DIR}/ssl/notif${name}.keytab`,
            `notif${name}@${REALM}`,
            // MIT requires a subsidiary cache name to begin with "tkt"
            '-c', `DIR::${collectionDir}/tkt${name}`,
        ], err => next(err));
    }, cb);
}

describe('notification delivery pool, kerberos destinations', function kerberosSuite() {
    this.timeout(ARM_TIMEOUT);

    before(function checkRig(done) {
        if (!rigControlAvailable()) {
            // without the broker log there is no evidence, so running the
            // arms would prove nothing
            return this.skip();
        }
        assert.ok(process.env.CONF_DIR,
            'CONF_DIR must point at a directory whose ssl/ holds the keytabs');
        return done();
    });

    let armStartedAt;
    let pool;

    beforeEach(done => {
        armStartedAt = Math.floor(Date.now() / 1000);
        setTimeout(done, ARM_GAP_MS);
    });

    afterEach(done => {
        // here rather than at the end of arm C, so that an arm C that threw
        // still leaves the environment as every other arm expects it
        restoreArmCEnvironment();
        if (!pool) {
            return done();
        }
        const closing = pool;
        pool = null;
        return closing.closeAll(done);
    });

    /**
     * Broker side evidence for the arm that just ran
     * @param {function} cb - cb(err, principals)
     * @return {undefined}
     */
    function brokerEvidence(cb) {
        containerLogSince(KAFKA_CONTAINER, armStartedAt, (err, logText) => {
            if (err) {
                return cb(err);
            }
            return cb(null, authenticatedPrincipals(logText));
        });
    }

    /**
     * Assert that both principals authenticated and both destinations
     * delivered everything they sent
     * @param {Object} results - runRounds results
     * @param {string[]} principals - broker side authentication ids
     * @param {number} rounds - rounds run
     * @return {undefined}
     */
    function assertBothIdentities(results, principals, rounds) {
        assert.ok(principals.includes(`notifa@${REALM}`),
            `broker never authenticated notifa, saw ${JSON.stringify(principals)}`);
        assert.ok(principals.includes(`notifb@${REALM}`),
            `broker never authenticated notifb, saw ${JSON.stringify(principals)}`);
        assert.deepStrictEqual(results.a.errors, []);
        assert.deepStrictEqual(results.b.errors, []);
        assert.strictEqual(results.a.delivered, rounds);
        assert.strictEqual(results.b.delivered, rounds);
    }

    describe('arm A, credential cache collection', () => {
        it('should let both destinations keep their own identity', done => {
            const collectionDir = `/tmp/krb-arm-a-${RUN_ID}`;
            kinitCollection(collectionDir, kinitErr => {
                assert.ifError(kinitErr);
                pool = makePool({ kerberosProducer: 'kafkajs', credentialSource: 'ccache' });
                runRounds(pool, { arm: 'A' }, (err, results) => {
                    assert.ifError(err);
                    brokerEvidence((logErr, principals) => {
                        assert.ifError(logErr);
                        assertBothIdentities(results, principals, ROUNDS);
                        delete process.env.KRB5CCNAME;
                        done();
                    });
                });
            });
        });
    });

    describe('arm B, client keytab with no kinit', () => {
        it('should obtain a ticket per principal without any kinit', done => {
            pool = makePool({ kerberosProducer: 'kafkajs' });
            runRounds(pool, { arm: 'B' }, (err, results) => {
                assert.ifError(err);
                brokerEvidence((logErr, principals) => {
                    assert.ifError(logErr);
                    assertBothIdentities(results, principals, ROUNDS);
                    done();
                });
            });
        });
    });

    describe('arm C, control on the shipping producer', () => {
        it('should collide on one identity and fail the other destination', done => {
            // node-rdkafka takes the process default credential, so this is
            // the behaviour the new producer exists to fix. The client
            // keytab an earlier arm configured is cleared, so that the
            // cache written just below is the only credential there is.
            delete process.env.KRB5_CLIENT_KTNAME;
            process.env.KRB5CCNAME = `FILE:${ARM_C_CACHE}`;
            const kept = `notif${ARM_C_PRINCIPAL}@${REALM}`;
            const denied = ARM_C_PRINCIPAL === 'a' ? 'b' : 'a';
            // the real kinit, before the no-op one goes on PATH
            execFile('kinit', [
                '-k', '-t',
                `${process.env.CONF_DIR}/ssl/notif${ARM_C_PRINCIPAL}.keytab`,
                kept,
            ], kinitErr => {
                assert.ifError(kinitErr);
                installNoopKinit();
                pool = makePool({ kerberosProducer: 'rdkafka' });
                runRounds(pool, {
                    arm: 'C', rounds: ARM_C_ROUNDS, roundMs: 3000,
                }, (err, results) => {
                    assert.ifError(err);
                    brokerEvidence((logErr, principals) => {
                        assert.ifError(logErr);
                        const distinct = [...new Set(principals)];
                        assert.deepStrictEqual(distinct, [kept],
                            'expected both connections to authenticate as the ' +
                            `one principal in the cache, saw ${JSON.stringify(principals)}`);
                        assert.deepStrictEqual(results[ARM_C_PRINCIPAL].errors, []);
                        assert.strictEqual(results[ARM_C_PRINCIPAL].delivered,
                            ARM_C_ROUNDS);
                        assert.strictEqual(results[denied].delivered, 0,
                            'the destination that lost its identity ' +
                            `delivered ${results[denied].delivered} of ` +
                            `${ARM_C_ROUNDS}`);
                        assert.strictEqual(results[denied].errors.length,
                            ARM_C_ROUNDS,
                            `expected every record to be denied, got ${JSON.stringify(
                                results[denied].errors)}`);
                        assert.ok(results[denied].errors.every(
                            error => /authoriz/i.test(error)),
                        `expected authorization failures, got ${JSON.stringify(
                            results[denied].errors)}`);
                        done();
                    });
                });
            });
        });
    });

    describe('arm D, one principal per process', () => {
        it('should deliver from both when each process holds one identity', done => {
            const runner = path.join(__dirname, 'kerberosSingleDestination.js');
            async.map(['a', 'b'], (name, next) => {
                const child = fork(runner, [name], {
                    env: { ...process.env, KRB_ROUNDS: '3', KRB_ROUND_MS: '2000' },
                    silent: false,
                });
                let payload = null;
                child.on('message', message => { payload = message; });
                child.on('exit', code => next(null, { name, code, payload }));
            }, (err, outcomes) => {
                assert.ifError(err);
                brokerEvidence((logErr, principals) => {
                    assert.ifError(logErr);
                    outcomes.forEach(outcome => {
                        assert.strictEqual(outcome.code, 0,
                            `child for notif${outcome.name} exited ${outcome.code}`);
                        assert.strictEqual(outcome.payload.delivered, 3,
                            `notif${outcome.name} delivered ` +
                            `${JSON.stringify(outcome.payload)}`);
                    });
                    assert.ok(principals.includes(`notifa@${REALM}`));
                    assert.ok(principals.includes(`notifb@${REALM}`));
                    done();
                });
            });
        });
    });

    describe('arm E, broker restarts', () => {
        it('should re-authenticate as its own principal after every restart', done => {
            pool = makePool({ kerberosProducer: 'kafkajs' });
            let restarts = 0;
            const restartTimer = setInterval(() => {
                restarts++;
                log.info('restarting the broker mid run', { restarts });
                restartContainer(KAFKA_CONTAINER, restartErr => {
                    if (restartErr) {
                        log.error('broker restart failed', { error: restartErr.message });
                    }
                });
                if (restarts >= 3) {
                    clearInterval(restartTimer);
                }
            }, 40000);
            runRounds(pool, { arm: 'E', rounds: 24, roundMs: 7000 }, (err, results) => {
                clearInterval(restartTimer);
                assert.ifError(err);
                brokerEvidence((logErr, principals) => {
                    assert.ifError(logErr);
                    // each restart forces a fresh handshake, so both
                    // principals must appear more than once
                    const perPrincipal = countByPrincipal(principals);
                    assert.ok(perPrincipal[`notifa@${REALM}`] > 1,
                        `notifa re-authenticated ${JSON.stringify(perPrincipal)}`);
                    assert.ok(perPrincipal[`notifb@${REALM}`] > 1,
                        `notifb re-authenticated ${JSON.stringify(perPrincipal)}`);
                    // a send that lands while the broker is down is a broker
                    // availability matter, but an authorization failure would
                    // mean a producer came back as the wrong principal, which
                    // is what this arm is actually testing
                    const errors = results.a.errors.concat(results.b.errors);
                    assert.deepStrictEqual(
                        errors.filter(error => /authoriz/i.test(error)), []);
                    assert.ok(results.a.delivered >= 20,
                        `notifa delivered ${results.a.delivered}/24, ` +
                        `errors ${JSON.stringify(results.a.errors)}`);
                    assert.ok(results.b.delivered >= 20,
                        `notifb delivered ${results.b.delivered}/24, ` +
                        `errors ${JSON.stringify(results.b.errors)}`);
                    done();
                });
            });
        });
    });

    describe('arm F, short ticket lifetime', () => {
        const setMaxLife = (lifetime, cb) => async.eachSeries(['a', 'b'], (name, next) =>
            execInContainer(KDC_CONTAINER, ['kadmin.local', '-q',
                `modprinc -maxlife "${lifetime}" notif${name}@${REALM}`],
            err => next(err)), cb);

        after(done => setMaxLife('1 day', done));

        it('should reacquire each principal as its ticket expires', done => {
            setMaxLife('2 minutes', lifeErr => {
                assert.ifError(lifeErr);
                // a pool per batch, so every batch starts from a fresh SASL
                // handshake and a ticket that expired has to be obtained again
                const batch = (arm, next) => {
                    const batchPool = makePool({ kerberosProducer: 'kafkajs' });
                    return runRounds(batchPool, {
                        arm, rounds: 3, roundMs: 12000,
                    }, (err, results) => batchPool.closeAll(
                        () => next(err, results)));
                };
                async.mapSeries(['F', 'F', 'F', 'F', 'F'], batch, (err, batches) => {
                    assert.ifError(err);
                    brokerEvidence((logErr, principals) => {
                        assert.ifError(logErr);
                        const perPrincipal = countByPrincipal(principals);
                        assert.ok(perPrincipal[`notifa@${REALM}`] >= 5,
                            `notifa handshakes ${JSON.stringify(perPrincipal)}`);
                        assert.ok(perPrincipal[`notifb@${REALM}`] >= 5,
                            `notifb handshakes ${JSON.stringify(perPrincipal)}`);
                        batches.forEach((results, index) => {
                            assert.deepStrictEqual(results.a.errors, [],
                                `batch ${index} destination a`);
                            assert.deepStrictEqual(results.b.errors, [],
                                `batch ${index} destination b`);
                        });
                        done();
                    });
                });
            });
        });
    });

    describe('arm G, footprint', () => {
        it('should stay flat in threads as producers are added', done => {
            // 50 destinations over the two principals, to size the cost of a
            // process holding many kerberised endpoints
            const many = {};
            for (let index = 0; index < 50; index++) {
                const name = index % 2 === 0 ? 'a' : 'b';
                many[`krb-many-${index}`] = {
                    ...destination(name, 'keytab'),
                    resource: `krb-many-${index}`,
                };
            }
            const manyPool = new DeliveryProducerPool({
                destinationsById: many,
                deliveryPoolConfig: {
                    deliveryTimeoutMs: 30000,
                    producerIdleMs: 300000,
                    maxProducers: 60,
                    kerberosProducer: 'kafkajs',
                },
                logger: log,
            });
            const footprint = () => ({
                rssMB: Math.round(process.memoryUsage().rss / 1048576),
                threads: fs.readdirSync('/proc/self/task').length,
            });
            const before = footprint();
            async.eachLimit(Object.keys(many), 10, (id, next) => {
                manyPool.get(id, (err, producer) => {
                    if (err) {
                        return next(err);
                    }
                    return producer.send([{
                        key: `G-${id}`,
                        message: JSON.stringify({
                            arm: 'G', producer: id, round: 1, runId: RUN_ID,
                        }),
                    }], next);
                });
            }, err => {
                assert.ifError(err);
                const after = footprint();
                log.info('footprint for 50 kerberised producers in one process', {
                    beforeRssMB: before.rssMB, afterRssMB: after.rssMB,
                    beforeThreads: before.threads, afterThreads: after.threads,
                });
                // kafkajs is pure JS and the gssapi calls happen at handshake
                // time only, so producer count must not cost threads
                assert.strictEqual(after.threads, before.threads);
                manyPool.closeAll(done);
            });
        });
    });

    describe('read back', () => {
        it('should find on the broker what the delivery reports claimed', done => {
            async.map(['topic-a', 'topic-b'], readBack, (err, counts) => {
                assert.ifError(err);
                const [topicA, topicB] = counts;
                log.info('read back', { topicA, topicB });
                // arm A and B each ran ROUNDS rounds per destination
                assert.strictEqual(topicA['A:a'], ROUNDS);
                assert.strictEqual(topicB['A:b'], ROUNDS);
                assert.strictEqual(topicA['B:a'], ROUNDS);
                assert.strictEqual(topicB['B:b'], ROUNDS);
                // arm E ran 24 rounds per destination across three restarts
                assert.ok(topicA['E:a'] >= 20, `arm E topic-a ${topicA['E:a']}`);
                assert.ok(topicB['E:b'] >= 20, `arm E topic-b ${topicB['E:b']}`);
                // arm C held one principal in the cache for both producers,
                // so its destination landed every record and the one that
                // lost its identity landed nothing at all
                const armC = { a: topicA['C:a'], b: topicB['C:b'] };
                const denied = ARM_C_PRINCIPAL === 'a' ? 'b' : 'a';
                assert.strictEqual(armC[ARM_C_PRINCIPAL], ARM_C_ROUNDS,
                    `arm C ${ARM_C_PRINCIPAL} landed ${armC[ARM_C_PRINCIPAL]}`);
                assert.strictEqual(armC[denied], undefined,
                    `arm C ${denied} lost its identity and still landed ` +
                    `${armC[denied]}`);
                done();
            });
        });
    });
});
