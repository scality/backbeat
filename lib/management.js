const async = require('async');
const forge = require('node-forge');
const arsenal = require('arsenal');
const config = require('../conf/Config');

const MetadataFileClient = arsenal.storage.metadata.MetadataFileClient;

let initialized = false;
let serviceCredentials = {
    accounts: [],
};

const tokenKey = 'auth/zenko/remote-management-token';

// XXX copy-pasted from S3
function decryptSecret(instanceCredentials, secret) {
    // XXX don't forget to use u.encryptionKeyVersion if present
    const privateKey = forge.pki.privateKeyFromPem(
        instanceCredentials.privateKey);
    const encryptedSecretKey = forge.util.decode64(secret);
    return privateKey.decrypt(encryptedSecretKey, 'RSA-OAEP', {
        md: forge.md.sha256.create(),
    });
}

function saveServiceCredentials(conf, auth) {
    const confObject = JSON.parse(conf);
    const instanceAuth = JSON.parse(auth);
    const serviceAccount = (confObject.users || []).find(
        u => u.accountType === 'service-replication');
    if (!serviceAccount) {
        return;
    }

    const secretKey = decryptSecret(instanceAuth, serviceAccount.secretKey);
    serviceCredentials = {
        accounts: [{
            name: 'service-replication',
            arn: 'aws::iam:234456789012:root',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            displayName: serviceAccount.userName,
            keys: {
                access: serviceAccount.accessKey,
                secret: secretKey,
            },
        }],
    };
    initialized = true;
}

function initManagement(done) {
    if (initialized) {
        return process.nextTick(done);
    }

    const mdClient = new MetadataFileClient({
        host: config.queuePopulator.dmd.host,
        port: config.queuePopulator.dmd.port,
    });

    const mdDb = mdClient.openDB(error => {
        if (error) {
            return process.nextTick(done, error);
        }

        const db = mdDb.openSub('PENSIEVE');
        return async.waterfall([
            cb => db.get('configuration/overlay-version', {}, cb),
            (version, cb) => db.get(`configuration/overlay/${version}`, {}, cb),
            (conf, cb) => db.get(tokenKey, {}, (err, instanceAuth) => {
                if (err) {
                    return cb(err);
                }
                saveServiceCredentials(conf, instanceAuth);
                return cb();
            }),
        ], done);
    });

    return undefined;
}

function getLatestServiceAccountCredentials() {
    return serviceCredentials;
}

module.exports = {
    initManagement,
    getLatestServiceAccountCredentials,
};
