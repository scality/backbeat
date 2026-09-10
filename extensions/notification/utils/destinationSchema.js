const joi = require('joi');

const { supportedSaslProtocols, supportedScramMechanisms } =
    require('../constants');

// The shape of one external destination, on its own so that both the
// extension configuration and the workgroups document validate a destination
// against the same schema: an assume-destination workgroup carries its
// destination in the document rather than in the destination registry, and
// two schemas for one shape would drift.

const sslSchema = joi.object({
    ssl: joi.boolean().default(false),
    ca: joi.string(),
    client: joi.string(),
    key: joi.string(),
    keyPassword: joi.string(),
});

const saslAuthSchema = sslSchema.append({
    protocol: joi.string().valid(...supportedSaslProtocols).required(),
});

const kerberosAuthSchema = saslAuthSchema.append({
    type: joi.string().valid('kerberos').required(),
    keytab: joi.string().required(),
    principal: joi.string().required(),
    serviceName: joi.string().required(),
});

const basicAuthBaseSchema = saslAuthSchema.append({
    type: joi.string().valid('basic').required(),
});

const basicAuthSchema = joi.alternatives().try(
    basicAuthBaseSchema.append({
        credentialsFile: joi.string().required(),
    }),
    basicAuthBaseSchema.append({
        username: joi.string().required(),
        password: joi.string().required(),
    }),
);

const scramAuthBaseSchema = saslAuthSchema.append({
    type: joi.string().valid('scram').required(),
    mechanism: joi.string().valid(...supportedScramMechanisms).required(),
});

const scramAuthSchema = joi.alternatives().try(
    scramAuthBaseSchema.append({
        credentialsFile: joi.string().required(),
    }),
    scramAuthBaseSchema.append({
        username: joi.string().required(),
        password: joi.string().required(),
    }),
);

const credentialsFileSchema = joi.object({
    username: joi.string().required(),
    password: joi.string().required(),
});

const authSchema = joi.alternatives().try(sslSchema, kerberosAuthSchema, basicAuthSchema, scramAuthSchema).default({});

const destinationSchema = joi.object({
    resource: joi.string().required(),
    type: joi.string().required(),
    host: joi.string().required(),
    port: joi.number().optional(),
    internalTopic: joi.string(),
    topic: joi.string().required(),
    auth: authSchema,
    requiredAcks: joi.number().when('type', {
        is: joi.string().not('kafka'),
        then: joi.forbidden(),
        otherwise: joi.number().default(1),
    }),
    compressionType: joi.string().when('type', {
        is: joi.string().not('kafka'),
        then: joi.forbidden(),
        otherwise: joi.string().default('none'),
    }),
    // number of record keys the destination is spread over: raise it to let
    // more than one delivery worker handle the destination in parallel;
    // keys collide under the broker's crc32(key) % partitions, so m keys
    // reach at most m partitions and usually fewer: size well above the
    // parallelism wanted and verify against the observed partition map
    spreadFactor: joi.number().integer().min(1).default(1),
});

module.exports = {
    authSchema,
    credentialsFileSchema,
    destinationSchema,
};
