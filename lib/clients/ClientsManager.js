const CredentialsManager = require('../credentials/CredentialsManager');
const BackbeatMetadataProxy = require('../BackbeatMetadataProxy');
const { createBackbeatClient, createS3Client } = require('./utils');
const { authTypeAssumeRole } = require('../constants');
const { http: HttpAgent, https: HttpsAgent } = require('httpagent');

const DELETE_INACTIVE_CREDENTIALS_INTERVAL = 1000 * 60 * 30; // 30m
const MAX_INACTIVE_DURATION = 1000 * 60 * 60 * 2; // 2hr

/**
 * @typedef {Object} StsConfig
 * @property {string} host - STS host
 * @property {string} port - STS port
 * @property {string} [externalFile] - Path of the file containing the STS crendentials
 * @property {string} [accessKey] - STS access key
 * @property {string} [secretKey] - STS secret key
 */

/**
 * @typedef {Object} AuthConfig
 * @property {('account'|'service'|'assumeRole')} type - Authentication method
 * @property {String} roleName - Role name to use in assumeRole
 * @property {StsConfig} sts - STS config
 */

/**
 * @typedef {Object} S3Config
 * @property {string} host - S3 host
 * @property {string} port - S3 port
 */

/**
 * @typedef {('http'|'https')} Transport
 */

/**
 * @typedef {Object} Config
 * @property {String} accountId - account id
 * @property {AuthConfig} authConfig - auth config
 * @property {S3Config} s3Config - S3 config
 * @property {Transport} transport - transport method
 */

/**
 * @typedef {Object} ClientConfig
 * @property {String} accountId - account id
 * @property {AuthConfig} authConfig - auth config
 * @property {S3Config} s3Config - S3 config
 * @property {Transport} transport - transport method
 * @property {(HttpAgent.Agent|HttpsAgent.Agent)} s3Agent - http agent to use for S3
 * @property {(HttpAgent.Agent|HttpsAgent.Agent)} stsAgent - http agent to use for STS
 */

/**
 * An improved version of the ClientManager Class
 *
 * Supports storing multiple clients with different
 * auth configs.
 */
class ClientsManager {

    /**
     * @constructor
     *
     * @param {String} id id of the clients manager
     * (used in the logs and as a suffix of the role session name)
     * @param {Object} logger logger instance
     */
    constructor(id, logger) {
        this._log = logger;
        this._id = id;
        this._configs = {};
        this.s3Clients = {};
        this.backbeatClients = {};
        this.credentialsManager = new CredentialsManager(this._id, this._log);
    }

    /**
     * Initializes the mecanism of credential deletion
     * after they expires
     * @returns {undefined}
     */
    initCredentialsManager() {
        this.credentialsManager.on('deleteCredentials', clientId => {
            delete this.s3Clients[clientId];
            delete this.backbeatClients[clientId];
        });

        this._deleteInactiveCredentialsInterval = setInterval(() => {
            this.credentialsManager.removeInactiveCredentials(MAX_INACTIVE_DURATION);
        }, DELETE_INACTIVE_CREDENTIALS_INTERVAL);
    }

    /**
     * @param {AuthConfig} authConfig
     * @param {Transport} transport
     * @param {HttpAgent.Agent|HttpsAgent.Agent} stsAgent
     * @returns {Object}
     */
    _generateSTSConfig(authConfig, transport, stsAgent) {
        if (authConfig.type === authTypeAssumeRole) {
            const { sts } = authConfig;
            const stsWithCreds = CredentialsManager.resolveExternalFileSync(sts, this._log);
            return {
                endpoint: `${transport||'http'}://${sts.host}:${sts.port}`,
                credentials: {
                    accessKeyId: stsWithCreds.accessKey,
                    secretAccessKey: stsWithCreds.secretKey,
                },
                region: 'us-east-1',
                signatureVersion: 'v4',
                sslEnabled: transport === 'https',
                httpOptions: { agent: stsAgent, timeout: 0 },
                maxRetries: 0,
            };
        }
        return null;
    }

    /**
     * @param {Transport} transport
     * @returns {Object}
     */
    _createAgents(transport) {
        if (transport === 'https') {
            return {
                s3Agent: new HttpsAgent.Agent({ keepAlive: true }),
                stsAgent: new HttpsAgent.Agent({ keepAlive: true }),
            };
        }
        return {
            s3Agent: new HttpAgent.Agent({ keepAlive: true }),
            stsAgent: new HttpAgent.Agent({ keepAlive: true }),
        };
    }

    /**
     * @param {Config} config
     * @returns {ClientConfig}
     */
    _generateClientConfig(config) {
        const { accountId, authConfig, s3Config, transport } = config;
        const { s3Agent, stsAgent } = this._createAgents(transport);
        const stsConfig = this._generateSTSConfig(authConfig, transport, stsAgent);
        return {
            accountId,
            authConfig,
            s3Config,
            transport: transport || 'http',
            stsConfig,
            s3Agent,
            stsAgent,
        };
    }

    /**
     * @param {String} clientId 
     * @param {Config} config 
     */
    setClientConfig(clientId, config) {
        this._configs[clientId] = this._generateClientConfig(config);
    }

    /**
     * @param {String} clientId 
     * @returns {ClientConfig|null}
     */
    getClientConfig(clientId) {
        return this._configs[clientId] || null;
    }

    /**
     * Return an S3 client instance
     * @param {String} clientId - The client id.
     * @return {AWS.S3} The S3 client instance to make requests with
     */
    getS3Client(clientId) {
        const config = this._configs[clientId];
        if (!config) {
            return null;
        }

        const credentials = this.credentialsManager.getCredentials({
            id: clientId,
            accountId: config.accountId,
            stsConfig: config.stsConfig,
            authConfig: config.authConfig,
        });

        if (credentials === null) {
            return null;
        }

        const client = this.s3Clients[clientId];

        if (client) {
            return client;
        }

        this.s3Clients[clientId] = createS3Client({
            transport: config.transport,
            port: config.s3Config.port,
            host: config.s3Config.host,
            credentials,
            agent: config.s3Agent,
        });

        return this.s3Clients[clientId];
    }

    /**
     * Return an backbeat metadata proxy
     * @param {String} clientId - The client id. .
     * @return {BackbeatMetadataProxy} The S3 client instance to make requests with
     */
    getBackbeatMetadataProxy(clientId) {
        const client = this.getBackbeatClient(clientId);
        if (client === null) {
            return null;
        }

        const config = this._configs[clientId];
        if (!config) {
            return null;
        }

        const { transport, s3Config, authConfig } = config;

        return new BackbeatMetadataProxy(
            `${transport}://${s3Config.host}:${s3Config.port}`,
            authConfig,
        ).setBackbeatClient(client);
    }

    /**
     * Return an backbeat client instance
     * @param {String} clientId - The client id.
     * @return {BackbeatClient} The S3 client instance to make requests with
     */
    getBackbeatClient(clientId) {
        const config = this._configs[clientId];
        if (!config) {
            return null;
        }

        const credentials = this.credentialsManager.getCredentials({
            id: clientId,
            accountId: config.accountId,
            stsConfig: config.stsConfig,
            authConfig: config.authConfig,
        });

        if (credentials === null) {
            return null;
        }

        const client = this.backbeatClients[clientId];

        if (client) {
            return client;
        }

        this.backbeatClients[clientId] = createBackbeatClient({
            transport: config.transport,
            port: config.s3Config.port,
            host: config.s3Config.host,
            credentials,
            agent: config.s3Agent,
        });

        return this.backbeatClients[clientId];
    }

    /**
     * Delete BackbeatClient and S3Client of
     * a specific ClientID
     * @param {String} clientId 
     * @returns {undefined}
     */
    removeClients(clientId) {
        delete this.s3Clients[clientId];
        delete this.backbeatClients[clientId];
    }
}

module.exports = ClientsManager;
