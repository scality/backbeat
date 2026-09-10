const assert = require('assert');

const FakeLogger = require('../../utils/fakeLogger');

const {
    gssapiAuthenticationProvider,
    gssapiSaslOption,
    readSaslBytes,
    writeSaslBytes,
    MAX_TOKEN_EXCHANGES,
} = require('../../../extensions/notification/destination/saslGssapi');

const PRINCIPAL = 'notifications@EXAMPLE.COM';

/**
 * A GSSAPI client that plays out a scripted token exchange, standing in for
 * the native binding
 */
class StubGssClient {
    /**
     * @constructor
     * @param {Object} [params] - stub params
     * @param {number} [params.steps] - how many steps before the security
     *   context completes
     * @param {Error} [params.stepError] - error thrown by the last step
     * @param {Error} [params.unwrapError] - error thrown by unwrap
     */
    constructor(params) {
        const options = params || {};
        this._stepsToComplete = options.steps === undefined ? 2 : options.steps;
        this._stepError = options.stepError;
        this._unwrapError = options.unwrapError;
        this.username = PRINCIPAL;
        this.contextComplete = false;
        this.calls = [];
    }

    async step(challenge) {
        this.calls.push(['step', challenge]);
        if (this._stepError && this.calls.length >= this._stepsToComplete) {
            throw this._stepError;
        }
        const stepNumber = this.calls.filter(call => call[0] === 'step').length;
        if (stepNumber >= this._stepsToComplete) {
            this.contextComplete = true;
            // a krb5 context that completes on the mutual auth reply has no
            // further token to send
            return '';
        }
        return Buffer.from(`client-token-${stepNumber}`).toString('base64');
    }

    async unwrap(challenge) {
        this.calls.push(['unwrap', challenge]);
        if (this._unwrapError) {
            throw this._unwrapError;
        }
        return Buffer.from('layer-challenge').toString('base64');
    }

    async wrap(challenge, options) {
        this.calls.push(['wrap', challenge, options]);
        return Buffer.from('client-final').toString('base64');
    }
}

/**
 * A kerberos binding whose initializeClient hands out the given client
 * @param {StubGssClient} client - client to hand out
 * @return {Object} stub binding
 */
function stubKerberos(client) {
    return {
        GSS_MECH_OID_KRB5: 9,
        GSS_C_MUTUAL_FLAG: 2,
        GSS_C_SEQUENCE_FLAG: 8,
        GSS_C_INTEG_FLAG: 32,
        initializeClientCalls: [],
        async initializeClient(service, options) {
            this.initializeClientCalls.push([service, options]);
            return client;
        },
    };
}

/**
 * A saslAuthenticate that replies with the given broker tokens in order
 * @param {Buffer[]} brokerTokens - raw tokens the broker sends back
 * @return {Object} { saslAuthenticate, sent }
 */
function stubSaslAuthenticate(brokerTokens) {
    const sent = [];
    let round = 0;
    const saslAuthenticate = async ({ request, response }) => {
        sent.push(readSaslBytes(await request.encode()));
        const token = brokerTokens[round] || Buffer.alloc(0);
        round++;
        // kafkajs hands response.decode the length prefixed field
        return response.parse(await response.decode(writeSaslBytes(token)));
    };
    return { saslAuthenticate, sent };
}

/**
 * Run one authentication
 * @param {Object} params - { client, brokerTokens, authorizationId }
 * @return {Promise} resolves to { client, kerberos, sent }
 */
async function authenticate(params) {
    const client = params.client || new StubGssClient();
    const kerberos = stubKerberos(client);
    const { saslAuthenticate, sent } = stubSaslAuthenticate(params.brokerTokens || [
        Buffer.from('broker-ap-rep'),
        Buffer.from('wrapped-layer-challenge'),
        Buffer.alloc(0),
    ]);
    const provider = gssapiAuthenticationProvider({
        kerberos,
        principal: PRINCIPAL,
        serviceName: 'kafka',
        authorizationId: params.authorizationId,
        logger: FakeLogger,
    });
    await provider({ host: 'broker.example.com', port: 9093, saslAuthenticate })
        .authenticate();
    return { client, kerberos, sent };
}

describe('notification saslGssapi', () => {
    describe('token framing', () => {
        it('should length prefix a token, because kafkajs writes it verbatim', () => {
            const framed = writeSaslBytes(Buffer.from('abc'));
            assert.strictEqual(framed.length, 7);
            assert.strictEqual(framed.readInt32BE(0), 3);
            assert.strictEqual(framed.subarray(4).toString(), 'abc');
        });

        it('should round trip a token through the framing', () => {
            const token = Buffer.from('a longer opaque gssapi token');
            assert.strictEqual(readSaslBytes(writeSaslBytes(token)).toString(),
                token.toString());
        });

        it('should frame an empty token as a zero length field', () => {
            const framed = writeSaslBytes(Buffer.alloc(0));
            assert.strictEqual(framed.length, 4);
            assert.strictEqual(framed.readInt32BE(0), 0);
        });

        it('should read an absent, empty or null broker token as empty', () => {
            [undefined, null, Buffer.alloc(0), Buffer.from([0, 0, 0, 0]),
                Buffer.from([0xff, 0xff, 0xff, 0xff])].forEach(raw =>
                assert.strictEqual(readSaslBytes(raw).length, 0));
        });
    });

    describe('authentication', () => {
        it('should acquire the credential for the configured principal', async () => {
            const { kerberos } = await authenticate({});
            assert.strictEqual(kerberos.initializeClientCalls.length, 1);
            const [service, options] = kerberos.initializeClientCalls[0];
            // host based service name, so the binding imports kafka/<host>
            assert.strictEqual(service, 'kafka@broker.example.com');
            assert.strictEqual(options.principal, PRINCIPAL);
            assert.strictEqual(options.mechOID, kerberos.GSS_MECH_OID_KRB5);
        });

        it('should request mutual authentication, sequencing and integrity', async () => {
            const { kerberos } = await authenticate({});
            const { flags } = kerberos.initializeClientCalls[0][1];
            assert.strictEqual(flags & kerberos.GSS_C_MUTUAL_FLAG,
                kerberos.GSS_C_MUTUAL_FLAG);
            assert.strictEqual(flags & kerberos.GSS_C_SEQUENCE_FLAG,
                kerberos.GSS_C_SEQUENCE_FLAG);
            assert.strictEqual(flags & kerberos.GSS_C_INTEG_FLAG,
                kerberos.GSS_C_INTEG_FLAG);
        });

        it('should start the exchange with an empty challenge', async () => {
            const { client } = await authenticate({});
            assert.deepStrictEqual(client.calls[0], ['step', '']);
        });

        it('should send the initial token, an empty token, then the final token',
            async () => {
                const { sent } = await authenticate({});
                assert.strictEqual(sent.length, 3);
                assert.strictEqual(sent[0].toString(), 'client-token-1');
                // once the context is complete the client has no token left,
                // and the empty request is what asks for the layer challenge
                assert.strictEqual(sent[1].length, 0);
                assert.strictEqual(sent[2].toString(), 'client-final');
            });

        it('should feed each broker token back into the security context',
            async () => {
                const { client } = await authenticate({
                    client: new StubGssClient({ steps: 3 }),
                    brokerTokens: [
                        Buffer.from('broker-1'),
                        Buffer.from('broker-2'),
                        Buffer.from('wrapped-layer-challenge'),
                        Buffer.alloc(0),
                    ],
                });
                const steps = client.calls.filter(call => call[0] === 'step');
                assert.strictEqual(steps.length, 3);
                assert.strictEqual(Buffer.from(steps[1][1], 'base64').toString(),
                    'broker-1');
                assert.strictEqual(Buffer.from(steps[2][1], 'base64').toString(),
                    'broker-2');
            });

        it('should unwrap the layer challenge and wrap the reply with the authzid',
            async () => {
                const { client } = await authenticate({});
                const unwrap = client.calls.find(call => call[0] === 'unwrap');
                const wrap = client.calls.find(call => call[0] === 'wrap');
                assert.strictEqual(Buffer.from(unwrap[1], 'base64').toString(),
                    'wrapped-layer-challenge');
                // the wrap is given what unwrap returned, not the raw challenge
                assert.strictEqual(Buffer.from(wrap[1], 'base64').toString(),
                    'layer-challenge');
                assert.deepStrictEqual(wrap[2], { user: PRINCIPAL });
            });

        it('should let the authorization id be set apart from the principal',
            async () => {
                const { client } = await authenticate({ authorizationId: 'notifications' });
                const wrap = client.calls.find(call => call[0] === 'wrap');
                assert.deepStrictEqual(wrap[2], { user: 'notifications' });
            });

        it('should not unwrap before the security context is complete', async () => {
            const { client } = await authenticate({
                client: new StubGssClient({ steps: 4 }),
                brokerTokens: [
                    Buffer.from('b1'), Buffer.from('b2'), Buffer.from('b3'),
                    Buffer.from('wrapped'), Buffer.alloc(0),
                ],
            });
            const kinds = client.calls.map(call => call[0]);
            assert.deepStrictEqual(kinds.slice(0, 4), ['step', 'step', 'step', 'step']);
            assert.strictEqual(kinds[4], 'unwrap');
        });
    });

    describe('error paths', () => {
        it('should surface an error from the security context', async () => {
            await assert.rejects(authenticate({
                client: new StubGssClient({ steps: 2, stepError: new Error('bad keytab') }),
            }), /bad keytab/);
        });

        it('should surface an error from the layer negotiation', async () => {
            await assert.rejects(authenticate({
                client: new StubGssClient({ unwrapError: new Error('bad mic') }),
            }), /bad mic/);
        });

        it('should give up rather than exchange tokens forever', async () => {
            // a context that never completes, as a broken peer would give
            const client = new StubGssClient({ steps: MAX_TOKEN_EXCHANGES + 5 });
            await assert.rejects(
                authenticate({ client, brokerTokens: [] }),
                new RegExp(`did not complete after ${MAX_TOKEN_EXCHANGES} token exchanges`));
        });

        it('should surface a broker rejection of the initial token', async () => {
            const kerberos = stubKerberos(new StubGssClient());
            const provider = gssapiAuthenticationProvider({
                kerberos, principal: PRINCIPAL, serviceName: 'kafka',
            });
            const saslAuthenticate = async () => {
                throw new Error('SASL authentication failed');
            };
            await assert.rejects(
                provider({ host: 'h', port: 9093, saslAuthenticate }).authenticate(),
                /SASL authentication failed/);
        });
    });

    describe('sasl option', () => {
        it('should name the mechanism GSSAPI, as the broker advertises it', () => {
            const option = gssapiSaslOption({
                kerberos: stubKerberos(new StubGssClient()),
                principal: PRINCIPAL,
                serviceName: 'kafka',
            });
            assert.strictEqual(option.mechanism, 'GSSAPI');
            assert.strictEqual(typeof option.authenticationProvider, 'function');
        });
    });
});
