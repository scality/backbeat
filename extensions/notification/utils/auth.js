const fs = require('fs');

const constants = require('../constants');

function getAuthFilePath(fileName) {
    const { authFilesFolder } = constants;
    if (process.env.CONF_DIR !== undefined) {
        const path = `${process.env.CONF_DIR}/${authFilesFolder}/${fileName}`;
        try {
            fs.accessSync(path, fs.F_OK | fs.R_OK);
            return path;
        } catch (e) {
            return null;
        }
    }
    return '';
}

/**
 * Generate auth options for kafka client
 * @param  {Object} auth - auth configuration for kafka client
 * @return {Object} returns auth object
 */
function generateKafkaAuthObject(auth) {
    const authObject = {};
    const { supportedAuthTypes } = constants;
    // TODO: auth object is fluid at the moment, once a solid structure is
    // defined, introduce a schema
    const {
        type,
        ssl,
        protocol,
        ca,
        client,
        key,
        keyPassword,
        keytab,
        principal,
        serviceName,
    } = auth;
    if (ssl) {
        authObject['security.protocol'] = 'ssl';
        const caPath = getAuthFilePath(ca);
        if (caPath) {
            authObject['ssl.ca.location'] = caPath;
        }
        const keyPath = getAuthFilePath(key);
        if (keyPath) {
            authObject['ssl.key.location'] = keyPath;
        }
        const clientPath = getAuthFilePath(client);
        if (clientPath) {
            authObject['ssl.certificate.location'] = clientPath;
        }
        authObject['ssl.key.password'] = keyPassword;
    }
    // only kereberos is supported now
    if (type && supportedAuthTypes.includes(type)) {
        authObject['security.protocol'] = protocol;
        authObject['sasl.kerberos.service.name'] = serviceName;
        authObject['sasl.kerberos.principal'] = principal;
        // optional, sasl protocols will have GSSAPI as their default mechanism
        authObject['sasl.mechanisms'] = 'GSSAPI';
        const keytabPath = getAuthFilePath(keytab);
        if (keytabPath) {
            authObject['sasl.kerberos.keytab'] = keytabPath;
            // default kinit command
            const kinitCommand = `kinit -k ${principal} -t ${keytabPath}`;
            authObject['sasl.kerberos.kinit.cmd'] = kinitCommand;
        }
    }
    return authObject;
}

module.exports = {
    generateKafkaAuthObject,
};
