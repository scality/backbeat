const AWS = require('aws-sdk');
const joi = require('@hapi/joi');

const BackbeatClient = require('./BackbeatClient');
const { transportJoi } = require('../config/configItems.joi');

const s3ProfileJoi = {
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
    constructor(clientCreateFn) {
        this._profiles = {};
        this._clients = {};
        this._clientCreateFn = clientCreateFn;
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
        delete this._profiles[profileName];
        delete this._clients[profileName];
    }

    /**
     * @callback ClientCreateFn
     * @param {object} profile - object contain the params to create the client
     * @returns {object|null} client
     */

    /**
    * get a client instance
    * Note that a disabled probe server does not pass an error to the callback.
    * @param {string} profileName - name of the profile to use to create the
    *   client
    * @param {object} log -
    * @param {ClientCreateFn} clientCreateFn - function to create a client
    * @returns {object} client
    */
    getClient(profileName, log) {
        if (!this._clientCreateFn || typeof this._clientCreateFn !== 'function') {
            log.error('invalid client create function', {
                method: 'ClientCache::getClient',
                profileName,
            });
            return null;
        }

        if (this._profiles[profileName] === undefined) {
            log.debug('unable to find profile', {
                method: 'ClientCache::getClient',
                profileName,
            });
            return null;
        }

        if (this._clients[profileName] === undefined) {
            const profile = this._profiles[profileName] || {};
            const client = this._clientCreateFn(profile, log);

            if (client === null) {
                return null;
            }

            this._clients[profileName] = client;
        }

        return this._clients[profileName];
    }
}

function validateProfile(profile, profileValidator) {
    try {
        const validProfile = joi.attempt(profile, profileValidator);
        return validProfile;
    } catch (err) {
        return { error: err };
    }
}

function createS3Client(profile, log) {
    const p = validateProfile(profile, s3ProfileJoi);

    if (p.error) {
        log.error('invalid client profile', {
            method: 'getS3Client',
            error: profile.error,
            profile: {
                transport: profile.transport,
                host: profile.host,
                port: profile.port,
                credentials: !!profile.credentials ? 'xxxx' : undefined,
                agent: profile.agent,
            },
        });
        return null;
    }

    const { transport, host, port, credentials, agent } = p;
    return new AWS.S3({
        endpoint: `${transport}://${host}:${port}`,
        credentials,
        sslEnabled: transport === 'https',
        s3ForcePathStyle: true,
        signatureVersion: 'v4',
        httpOptions: { agent, timeout: 0 },
        maxRetries: 0,
    });
}

function createBackbeatClient(profile, log) {
    const p = validateProfile(profile, s3ProfileJoi);

    if (p.error) {
        log.error('invalid client profile', {
            method: 'createBackbeatClient',
            error: profile.error,
            profile: {
                transport: profile.transport,
                host: profile.host,
                port: profile.port,
                credentials: !!profile.credentials ? 'xxxx' : undefined,
                agent: profile.agent,
            },
        });
        return null;
    }

    const { transport, host, port, credentials, agent } = p;
    const endpoint = `${transport}://${host}:${port}`;
    return new BackbeatClient({
        endpoint,
        credentials,
        sslEnabled: transport === 'https',
        httpOptions: { agent, timeout: 0 },
        maxRetries: 0,
    });
}

module.exports = {
    ClientCache,
    createS3Client,
    createBackbeatClient,
};
