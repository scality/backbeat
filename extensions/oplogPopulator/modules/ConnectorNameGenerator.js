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
 * Names the connectors of the oplog populator, and tells which ones
 * it owns: every connector name starting with
 * `<prefix>source-connector-`
 */
class ConnectorNameGenerator {
    /**
     * @constructor
     * @param {string} [prefix] configured connector prefix
     */
    constructor(prefix) {
        // `<prefix>source-connector-`, which every name starts with
        this._namePrefix = `${prefix || ''}${defaultConnectorName}-`;
    }

    /**
     * Checks whether a connector is managed by the oplog populator
     * @param {string} name connector name
     * @returns {boolean} true if owned
     */
    isOwned(name) {
        return name.startsWith(this._namePrefix);
    }

    /**
     * Name of a hashed connector:
     * `<prefix>source-connector-<nbConnectors>-<index>-<pipeline hash>`,
     * so that any pipeline change yields a new connector name
     * @param {Object} params params
     * @param {number} params.nbConnectors number of hashed connectors
     * @param {number} params.index connector index
     * @param {string} params.pipeline connector pipeline, as a string
     * @returns {string} connector name
     */
    hashedName({ nbConnectors, index, pipeline }) {
        return `${this._namePrefix}${nbConnectors}-${index}-${pipelineHash(pipeline)}`;
    }

    /**
     * Parses a hashed connector name. Per-bucket connector names
     * (`<prefix>source-connector-<uuid>`) are not hashed connectors.
     * @param {string} name connector name
     * @returns {Object|null} `{ nbConnectors, index, hash }`, or null
     */
    parseHashed(name) {
        if (!this.isOwned(name)) {
            return null;
        }
        const match = nameSuffixRegex.exec(name.slice(this._namePrefix.length));
        if (!match) {
            return null;
        }
        const [nbConnectors, index] = match.slice(1, 3).map(Number);
        if (nbConnectors < 1 || index >= nbConnectors) {
            return null;
        }
        return { nbConnectors, index, hash: match[3] };
    }
}

module.exports = ConnectorNameGenerator;
