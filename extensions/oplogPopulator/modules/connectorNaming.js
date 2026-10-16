const crypto = require('crypto');
const { defaultConnectorName } = require('../constants');

const nameHashLength = 8;
const nameSuffixRegex = new RegExp(
    `^(\\d+)-(\\d+)-([0-9a-f]{${nameHashLength}})$`);

/**
 * Short hash of a connector pipeline
 * @param {string} pipeline connector pipeline, as a string
 * @returns {string} hash
 */
function pipelineHash(pipeline) {
    return crypto.createHash('sha256').update(pipeline).digest('hex').slice(0, nameHashLength);
}

/**
 * Prefix of every connector managed by the oplog populator
 * @param {string} prefix configured connector prefix
 * @returns {string} connector name prefix
 */
function ownedConnectorPrefix(prefix) {
    return `${prefix}${defaultConnectorName}-`;
}

/**
 * Name of a hashed connector:
 * `<prefix>source-connector-<nbConnectors>-<index>-<pipeline hash>`,
 * so that any pipeline change yields a new connector name
 * @param {Object} params params
 * @param {string} params.prefix configured connector prefix
 * @param {number} params.nbConnectors number of hashed connectors
 * @param {number} params.index connector index
 * @param {string} params.pipeline connector pipeline, as a string
 * @returns {string} connector name
 */
function hashedConnectorName({ prefix, nbConnectors, index, pipeline }) {
    const hash = pipelineHash(pipeline);
    return `${ownedConnectorPrefix(prefix)}${nbConnectors}-${index}-${hash}`;
}

/**
 * Parses a hashed connector name. Per-bucket connector names
 * (`<prefix>source-connector-<uuid>`) are not hashed connectors.
 * @param {string} prefix configured connector prefix
 * @param {string} name connector name
 * @returns {Object|null} `{ nbConnectors, index, hash }`, or null
 */
function parseHashedConnectorName(prefix, name) {
    const ownedPrefix = ownedConnectorPrefix(prefix);
    if (!name.startsWith(ownedPrefix)) {
        return null;
    }
    const match = nameSuffixRegex.exec(name.slice(ownedPrefix.length));
    if (!match) {
        return null;
    }
    const [nbConnectors, index] = match.slice(1, 3).map(Number);
    if (nbConnectors < 1 || index >= nbConnectors) {
        return null;
    }
    return { nbConnectors, index, hash: match[3] };
}

module.exports = {
    ownedConnectorPrefix,
    hashedConnectorName,
    parseHashedConnectorName,
};
