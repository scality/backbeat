const constants = require('../constants');

/**
 * SASL GSSAPI (RFC 4752) as a kafkajs custom SASL mechanism.
 *
 * kafkajs ships PLAIN, SCRAM, AWS and OAUTHBEARER but no GSSAPI, so the
 * mechanism is implemented here on top of a GSSAPI binding. The binding is
 * injected rather than required at module load, so the state machine can be
 * unit tested without the native module.
 *
 * The credential is chosen per client object: the binding acquires it for the
 * principal named in `principal`, instead of taking whatever the process
 * default credential cache happens to hold. That is the whole reason this
 * exists next to the node-rdkafka producer, which has one identity per process.
 *
 * Token exchange against kafka's SaslAuthenticate framing:
 *
 *   client -> broker   initial token
 *   broker -> client   token, exchanged until the security context is complete
 *   client -> broker   empty token, to ask for the security layer challenge
 *   broker -> client   wrapped security layer bitmask and max buffer size
 *   client -> broker   wrapped reply selecting no security layer, plus authzid
 *   broker -> client   empty token, authentication complete
 */

// A malformed exchange must not loop forever: a krb5 context needs three
// client tokens, so anything past this is a broken peer.
const MAX_TOKEN_EXCHANGES = 12;

/**
 * Frame a SASL token the way kafkajs expects it.
 *
 * kafkajs writes the buffer it is given into the SaslAuthenticate request
 * verbatim, while the field is a kafka BYTES, so the length prefix is the
 * caller's job. Its own PLAIN mechanism does the same thing.
 *
 * @param {Buffer} token - raw SASL token
 * @return {Buffer} length prefixed token
 */
function writeSaslBytes(token) {
    const framed = Buffer.allocUnsafe(4 + token.length);
    framed.writeInt32BE(token.length, 0);
    token.copy(framed, 4);
    return framed;
}

/**
 * Strip the length prefix kafkajs puts back on the SaslAuthenticate response
 *
 * @param {Buffer} raw - length prefixed token, as handed to response.decode
 * @return {Buffer} raw SASL token, empty when the broker sent none
 */
function readSaslBytes(raw) {
    if (!raw || raw.length < 4) {
        return Buffer.alloc(0);
    }
    const length = raw.readInt32BE(0);
    if (length <= 0) {
        return Buffer.alloc(0);
    }
    return Buffer.from(raw.subarray(4, 4 + length));
}

/**
 * Build a kafkajs authenticationProvider that authenticates as one principal.
 *
 * @param {Object} params - provider params
 * @param {Object} params.kerberos - GSSAPI binding, the `kerberos` module or
 *   anything exposing initializeClient(service, options)
 * @param {string} params.principal - client principal to authenticate as,
 *   for example notifications@EXAMPLE.COM
 * @param {string} params.serviceName - kafka broker service name, the
 *   `kafka` part of kafka/broker.example.com@EXAMPLE.COM
 * @param {string} [params.authorizationId] - SASL authorization id, defaults
 *   to the principal
 * @param {Logger} [params.logger] - logger object
 * @return {function} kafkajs authenticationProvider
 */
function gssapiAuthenticationProvider(params) {
    const { kerberos, principal, serviceName, authorizationId, logger } = params;

    return ({ host, port, saslAuthenticate }) => ({
        authenticate: async () => {
            const context = { method: 'saslGssapi.authenticate', principal, host, port };
            // The service name has to be host based, so that the binding
            // imports it as kafka/<host>@<realm>, matching the broker keytab.
            const client = await kerberos.initializeClient(`${serviceName}@${host}`, {
                principal,
                mechOID: kerberos.GSS_MECH_OID_KRB5,
                flags: kerberos.GSS_C_MUTUAL_FLAG
                    | kerberos.GSS_C_SEQUENCE_FLAG
                    | kerberos.GSS_C_INTEG_FLAG,
            });

            const exchange = async tokenBase64 => {
                const token = tokenBase64 ? Buffer.from(tokenBase64, 'base64') : Buffer.alloc(0);
                const reply = await saslAuthenticate({
                    request: { encode: async () => writeSaslBytes(token) },
                    response: {
                        decode: async raw => readSaslBytes(raw),
                        parse: async decoded => decoded,
                    },
                });
                return Buffer.isBuffer(reply) ? reply.toString('base64') : '';
            };

            let clientToken = await client.step('');
            let brokerToken = '';
            let exchanges = 0;
            for (;;) {
                exchanges++;
                if (exchanges > MAX_TOKEN_EXCHANGES) {
                    throw new Error('GSSAPI security context did not complete ' +
                        `after ${MAX_TOKEN_EXCHANGES} token exchanges`);
                }
                brokerToken = await exchange(clientToken);
                if (client.contextComplete) {
                    break;
                }
                clientToken = await client.step(brokerToken);
            }

            // brokerToken now holds the wrapped security layer challenge.
            // Unwrapping it and wrapping it back with an authorization id is
            // the RFC 4752 client final message; the binding rewrites the
            // layer bitmask to "none" as part of the wrap.
            const challenge = await client.unwrap(brokerToken);
            const finalToken = await client.wrap(challenge, {
                user: authorizationId || principal,
            });
            await exchange(finalToken);

            if (logger) {
                logger.debug('authenticated to kafka destination over GSSAPI', {
                    ...context,
                    exchanges,
                    authenticatedAs: client.username,
                });
            }
        },
    });
}

/**
 * Build the kafkajs sasl option for one principal
 *
 * @param {Object} params - same params as gssapiAuthenticationProvider
 * @return {Object} kafkajs sasl configuration
 */
function gssapiSaslOption(params) {
    return {
        mechanism: constants.saslGssapiMechanism,
        authenticationProvider: gssapiAuthenticationProvider(params),
    };
}

module.exports = {
    gssapiAuthenticationProvider,
    gssapiSaslOption,
    readSaslBytes,
    writeSaslBytes,
    MAX_TOKEN_EXCHANGES,
};
