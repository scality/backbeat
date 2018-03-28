const fs = require('fs');
const async = require('async');
const { errors } = require('arsenal');

const VaultClientCache = require('../clients/VaultClientCache');

function getVaultCredentials(authConfig, canonicalId, extensionName, cb) {
    const { vault } = authConfig;
    const { host, port, adminPort, adminCredentialsFile } = vault;
    const adminCredsJSON = fs.readFileSync(adminCredentialsFile);
    const adminCredsObj = JSON.parse(adminCredsJSON);
    const accessKey = Object.keys(adminCredsObj)[0];
    const secretKey = adminCredsObj[accessKey];

    const vaultClientCache = new VaultClientCache();
    if (accessKey && secretKey) {
        vaultClientCache
            .setHost(`${extensionName}:admin`, host)
            .setPort(`${extensionName}:admin`, adminPort)
            .loadAdminCredentials(`${extensionName}:admin`,
                accessKey, secretKey);
    } else {
        throw new Error(`${extensionName} not properly configured: ` +
            'missing credentials for Vault admin client');
    }
    vaultClientCache
        .setHost(`${extensionName}:s3`, host)
        .setPort(`${extensionName}:s3`, port);

    const vaultClient = vaultClientCache.getClient(`${extensionName}:s3`);
    const vaultAdmin = vaultClientCache.getClient(`${extensionName}:admin`);
    return async.waterfall([
        // Get the account's display name for generating a new access key.
        next =>
            vaultClient.getAccounts(undefined, undefined, [canonicalId], {},
            (err, data) => {
                if (err) {
                    return next(err);
                }
                if (data.length !== 1) {
                    return next(errors.InternalError);
                }
                return next(null, data[0].name);
            }),
        // Generate a new account access key beacuse it has not been cached.
        (name, next) =>
            vaultAdmin.generateAccountAccessKey(name, (err, data) => {
                if (err) {
                    return next(err);
                }
                const accountCreds = {
                    accessKeyId: data.id,
                    secretAccessKey: data.value,
                };
                return next(null, accountCreds);
            }),
    ], (err, accountCreds) => cb(err, accountCreds));
}

module.exports = getVaultCredentials;
