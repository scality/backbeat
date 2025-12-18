const fs = require('fs');
const joi = require('joi');

const { authFilesFolder, supportedSaslProtocols } = require('../constants');
const { credentialsFileSchema } = require('../NotificationConfigValidator');

function getAuthFilePath(fileName) {
    if (process.env.CONF_DIR !== undefined) {
        const path = `${process.env.CONF_DIR}/${authFilesFolder}/${fileName}`;
        try {
            fs.accessSync(path, fs.F_OK | fs.R_OK);
            return path;
        } catch {
            return null;
        }
    }
    return null;
}

function readCredentialsFile(filePath) {
    const raw = fs.readFileSync(filePath, 'utf8');
    const data = JSON.parse(raw);
    return joi.attempt(data, credentialsFileSchema);
}

/**
 * Generate auth options for kafka client
 * @param  {Object} auth - auth configuration for kafka client
 * @return {Object} returns auth object
 */
function generateKafkaAuthObject(auth) {
    const authObject = {};

    const { type, ssl, ca, client, key } = auth;

    if (ssl) {
        authObject['security.protocol'] = 'ssl';
        if (ca) {
            const caPath = getAuthFilePath(ca);
            if (caPath === null) {
                throw new Error(`CA file ${ca} not found in ${authFilesFolder}`);
            }
            authObject['ssl.ca.location'] = caPath;
        }

        if (key) {
            authObject['ssl.key.password'] = auth.keyPassword;
            const keyPath = getAuthFilePath(key);
            if (keyPath === null) {
                throw new Error(`Key file ${key} not found in ${authFilesFolder}`);
            }
            authObject['ssl.key.location'] = keyPath;
        }

        if (client) {
            const clientPath = getAuthFilePath(client);
            if (clientPath === null) {
                throw new Error(`Client certificate file ${client} not found in ${authFilesFolder}`);
            }
            authObject['ssl.certificate.location'] = clientPath;
        }
    }

    switch (type) {
        case undefined:
            break;
        case 'kerberos': {
            const { protocol, serviceName, principal, keytab } = auth;
            if (!supportedSaslProtocols.includes(protocol)) {
                throw new Error(`Unsupported security.protocol: ${protocol}`);
            }
            // optional, sasl protocols will have GSSAPI as their default mechanism
            authObject['sasl.mechanisms'] = 'GSSAPI';
            authObject['security.protocol'] = protocol;
            authObject['sasl.kerberos.service.name'] = serviceName;
            authObject['sasl.kerberos.principal'] = principal;
            if (keytab) {
                const keytabPath = getAuthFilePath(keytab);
                if (keytabPath === null) {
                    throw new Error(`Keytab file ${keytab} not found in ${authFilesFolder}`);
                }
                authObject['sasl.kerberos.keytab'] = keytabPath;
                authObject['sasl.kerberos.kinit.cmd'] = `kinit -k ${principal} -t ${keytabPath}`;
            }
            break;
        }
        case 'basic': {
            const { protocol, credentialsFile, username, password } = auth;
            if (!supportedSaslProtocols.includes(protocol)) {
                throw new Error(`Unsupported security.protocol: ${protocol}`);
            }
            authObject['sasl.mechanisms'] = 'PLAIN';
            authObject['security.protocol'] = protocol;
            if (credentialsFile) {
                const credsFilePath = getAuthFilePath(credentialsFile);
                if (credsFilePath === null) {
                    throw new Error(`Credentials file ${credentialsFile} not found in ${authFilesFolder}`);
                }
                const credentials = readCredentialsFile(credsFilePath);
                authObject['sasl.username'] = credentials.username;
                authObject['sasl.password'] = credentials.password;
            } else {
                authObject['sasl.username'] = username;
                authObject['sasl.password'] = password;
            }
            break;
        }
        default: {
            throw new Error(`Unsupported auth type: ${type}`);
        }
    }

    return authObject;
}

module.exports = {
    generateKafkaAuthObject,
};
