'use strict';

/**
 * Reading and writing a configuration field from its path, shared by the
 * override mechanisms applied to the configuration before validation.
 */

/**
 * @param {Object} config - configuration to read from
 * @param {string[]} path - config path
 * @returns {*} value of the field, undefined if a node of the path is missing
 */
function getField(config, path) {
    return path.reduce((node, key) => node?.[key], config);
}

/**
 * Sets a config value, creating the missing intermediate nodes. A node holding
 * anything other than an object is reported rather than replaced.
 *
 * @param {Object} config - configuration to update
 * @param {string[]} path - config path
 * @param {*} value - value to set
 * @returns {undefined}
 */
function setField(config, path, value) {
    const parent = path.slice(0, -1).reduce((node, key, index) => {
        if (!node[key]) {
            node[key] = {}; // eslint-disable-line no-param-reassign
        } else if (typeof node[key] !== 'object' || Array.isArray(node[key])) {
            throw new Error(`cannot set ${path.join('.')}: ` +
                `${path.slice(0, index + 1).join('.')} is not an object`);
        }
        return node[key];
    }, config);
    parent[path[path.length - 1]] = value;
}

module.exports = {
    getField,
    setField,
};
