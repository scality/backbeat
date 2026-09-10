'use strict';

/**
 * Real S3 requests against CloudServer, so every event in the demo starts
 * life as an ordinary bucket operation.
 *
 * This uses @aws-sdk/client-s3, which backbeat already depends on, so the
 * suite needs nothing the repository does not already install. Two client
 * options matter against CloudServer 9.3: path-style addressing, because the
 * endpoint is a bare host and port, and checksums only when the operation
 * requires them, because the SDK's default of a CRC32 trailer on every
 * PutObject is not something a 9.3 server accepts.
 */

const fs = require('fs');
const {
    S3Client,
    CreateBucketCommand,
    PutBucketNotificationConfigurationCommand,
    GetBucketNotificationConfigurationCommand,
    PutObjectCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');
const env = require('./env');

/**
 * An S3 client for one endpoint.
 *
 * @param {String} [endpoint] - endpoint, default env.S3_ENDPOINT
 * @return {Object} an S3Client
 */
function client(endpoint) {
    return new S3Client({
        endpoint: endpoint || env.S3_ENDPOINT,
        region: 'us-east-1',
        forcePathStyle: true,
        credentials: {
            accessKeyId: env.S3_ACCESS_KEY,
            secretAccessKey: env.S3_SECRET_KEY,
        },
        maxAttempts: 3,
        // CloudServer 9.3 rejects the SDK's default aws-chunked CRC32
        // trailer, so ask for a checksum only where the operation needs one.
        requestChecksumCalculation: 'WHEN_REQUIRED',
        responseChecksumValidation: 'WHEN_REQUIRED',
    });
}

/**
 * The error code, whatever the SDK called it. v3 puts the service code in
 * `name`; a network or config failure has only a message.
 *
 * @param {Error} err - the error
 * @return {String} a code
 */
function codeOf(err) {
    return err.name || err.Code || err.code || 'Error';
}

function statusOf(err) {
    return (err.$metadata && err.$metadata.httpStatusCode) || err.statusCode;
}

/**
 * A notification configuration body.
 *
 * A rule token is a destination id, optionally `dest:prefix` for a key
 * prefix filter, or a full ARN used verbatim, which is how the name
 * collision case puts an account-scoped ARN. The rule id is what arrives in
 * the event as s3.configurationId, so the demo can show which rule produced
 * which event.
 *
 * CloudServer's filter rule names are case sensitive ("Prefix"), while the
 * matcher compares case-insensitively.
 *
 * @param {Array} tokens - rule tokens
 * @return {Object} the configuration body
 */
function notificationConfiguration(tokens) {
    return {
        QueueConfigurations: tokens.map(tok => {
            let arn;
            let prefix = null;
            let id;
            if (tok.startsWith('arn:')) {
                arn = tok;
                id = `${tok.split(':').pop()}-verbatim`;
            } else {
                const [dest, pre] = tok.split(':');
                arn = `arn:scality:bucketnotif:::${dest}`;
                prefix = pre || null;
                id = `${dest}-${prefix ? prefix.replace(/\/$/, '').replace(/\//g, '-') : 'all'}`;
            }
            const rule = {
                Id: id,
                QueueArn: arn,
                Events: ['s3:ObjectCreated:*', 's3:ObjectRemoved:*'],
            };
            if (prefix) {
                rule.Filter = { Key: { FilterRules: [{ Name: 'Prefix', Value: prefix }] } };
            }
            return rule;
        }),
    };
}

async function createBucket(s3, bucket) {
    try {
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        return 'created';
    } catch (err) {
        const code = codeOf(err);
        if (code === 'BucketAlreadyOwnedByYou'
            || code === 'BucketAlreadyExists') {
            return 'exists';
        }
        throw err;
    }
}

/**
 * Put a notification configuration and read it back.
 *
 * @param {Object} s3 - client
 * @param {String} bucket - bucket
 * @param {Array} tokens - rule tokens
 * @return {Object} { ok, code, message, readBack, body }
 */
async function putNotification(s3, bucket, tokens) {
    const body = notificationConfiguration(tokens);
    try {
        await s3.send(new PutBucketNotificationConfigurationCommand({
            Bucket: bucket,
            NotificationConfiguration: body,
        }));
    } catch (err) {
        return { ok: false, code: codeOf(err), statusCode: statusOf(err),
            message: err.message, body };
    }
    const readBack = await s3.send(new GetBucketNotificationConfigurationCommand({
        Bucket: bucket,
    }));
    return { ok: true, statusCode: 200, readBack, body };
}

/**
 * Create a bucket and put its notification configuration in one step.
 *
 * @param {Object} s3 - client
 * @param {String} bucket - bucket
 * @param {Array} tokens - rule tokens
 * @return {Object} the putNotification result
 */
async function bucketWith(s3, bucket, tokens) {
    await createBucket(s3, bucket);
    return putNotification(s3, bucket, tokens);
}

/**
 * One PUT, with the sequence in the object size.
 *
 * @param {Object} s3 - client
 * @param {String} bucket - bucket
 * @param {String} key - object key
 * @param {Number} size - body size, 1000 + sequence
 * @return {Promise} resolves when the PUT completes
 */
function put(s3, bucket, key, size) {
    return s3.send(new PutObjectCommand({ Bucket: bucket, Key: key,
        Body: Buffer.alloc(size, 0x61), ContentLength: size }));
}

function del(s3, bucket, key) {
    return s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
}

/**
 * Write the golden event, the shape the migration must preserve, out of a
 * topic dump.
 *
 * @param {String} dumpFile - a kafka dump
 * @param {String} outFile - where to write it
 * @return {Object|null} the event, or null if the dump has none
 */
function goldenEvent(dumpFile, outFile) {
    if (!fs.existsSync(dumpFile)) {
        return null;
    }
    const lines = fs.readFileSync(dumpFile, 'utf8').split('\n');
    for (const line of lines) {
        const parts = line.split('\t');
        if (!parts.length) {
            continue;
        }
        try {
            const doc = JSON.parse(parts[parts.length - 1]);
            if (doc && doc.Records && doc.Records.length) {
                fs.writeFileSync(outFile, `${JSON.stringify(doc, null, 2)}\n`);
                return doc;
            }
        } catch {
            // not an event line
        }
    }
    return null;
}

module.exports = {
    client,
    codeOf,
    notificationConfiguration,
    createBucket,
    putNotification,
    bucketWith,
    put,
    del,
    goldenEvent,
};
