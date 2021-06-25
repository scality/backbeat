const joi = require('@hapi/joi');

const { transportJoi } = require('../config/configItems.joi');

const profileJoi = {
    transport: transportJoi,
    host: joi.string().required(),
    port: joi.number().greater(0).required(),
    agent: joi.object().optional(),
    credentials: joi.object().required(),
};

/**
 * @class ClientCache
 *
 * @classdesc Helper class to manage a set of clients
 *
 * Clients share the same configuration based on the profile they
 * belong to. A profile is automatically created when it does not
 * exist and the first configuration is set for a given profile name.
 *
 * When getClient() is called, a new or cached client instance is
 * returned, based on if the profile name, host and port provided
 * match a cached instance. When client instances are created
 * they are cached and kept indefinitely.
 */
class ClientCache {
    constructor() {
        this._profiles = {};
        this._clients = {};
    }

    _updateProfile(profileName, params) {
        const profile = this._profiles[profileName] || {};
        this._profiles[profileName] = profile;

        Object.assign(profile, params);
        return this;
    }

    /**
     * Set the target service host name for the given profile
     *
     * @param {string} profileName - name of profile to bind to
     * @param {string} host - target service host
     * @return {ClientCache} this
     */
    setHost(profileName, host) {
        return this._updateProfile(profileName, { host });
    }

    /**
     * Set the target service port for the given profile
     *
     * @param {string} profileName - name of profile to bind to
     * @param {number} port - target service port
     * @return {ClientCache} this
     */
    setPort(profileName, port) {
        return this._updateProfile(profileName, { port });
    }

    /**
     * Set the target service transport protocol for the given profile
     *
     * @param {string} profileName - name of profile to bind to
     * @param {number} transport - target transport protocol
     * @return {ClientCache} this
     */
    setTransport(profileName, transport) {
        return this._updateProfile(profileName, { transport });
    }

    /**
     * Set the target service connection agent for the given profile
     *
     * @param {string} profileName - name of profile to bind to
     * @param {number} agent - target connection agent
     * @return {ClientCache} this
     */
    setAgent(profileName, agent) {
        return this._updateProfile(profileName, { agent });
    }

    /**
     * Set the target service auth credentials for the given profile
     *
     * @param {string} profileName - name of profile to bind to
     * @param {number} credentials - auth credentials
     * @return {ClientCache} this
     */
    setCredentials(profileName, credentials) {
        return this._updateProfile(profileName, { credentials });
    }

    deleteProfile(profileName) {
        delete this._profile[profileName];
        delete this._clients[profileName];
    }

    getProfile(profileName) {
        return this._profiles[profileName];
    }

    getValidProfile(profileName) {
        try {
            return joi.attempt(this._profiles[profileName], profileJoi);
        } catch (err) {
            return { error: err };
        }
    }
}

module.exports = ClientCache;
