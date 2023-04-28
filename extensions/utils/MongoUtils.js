const util = require('util');

/**
 * Constructs mongo connection config
 * @param {Object} mongoConfig mongo connection config
 * @param {Object} mongoConfig.authCredentials mongo auth config
 * @param {string} mongoConfig.authCredentials.username username
 * @param {string} mongoConfig.authCredentials.password password
 * @param {string} mongoConfig.replicaSetHosts replica set hosts
 * @param {string} mongoConfig.replicaSet replica set name
 * @param {string} mongoConfig.writeConcern write concern
 * @param {string} mongoConfig.readPreference read preference
 * @returns {string} mongo Connection config
 */
function constructConnectionString(mongoConfig) {
    const { authCredentials, replicaSetHosts, writeConcern,
        readPreference, replicaSet } = mongoConfig;
    let cred = '';
    if (authCredentials &&
        authCredentials.username &&
        authCredentials.password) {
        const username = encodeURIComponent(authCredentials.username);
        const password = encodeURIComponent(authCredentials.password);
        cred = `${username}:${password}@`;
    }
    let url = `mongodb://${cred}${replicaSetHosts}/` +
        `?w=${writeConcern}&readPreference=${readPreference}`;
    if (replicaSet) {
        url += `&replicaSet=${replicaSet}`;
    }
    return url;
}


/**
 * returns mongo version
 * @param {MongoClient} client mongo client
 * @param {Function} [callback] callback function
 * @returns {string|undefined} mongo version or
 * undefined if using the callback function
 */
async function getMongoVersion(client, callback) {
    async function getVersion() {
        const res = await client.command({
            buildInfo: 1,
        });
        return res.version;
    }
    if (callback) {
        return util.callbackify(getVersion)(callback);
    }
    return getVersion();
}

module.exports = {
    constructConnectionString,
    getMongoVersion,
};
