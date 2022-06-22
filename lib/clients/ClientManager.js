const http = require('http');
const https = require('https');

const CredentialsManager = require('../credentials/CredentialsManager');
const BackbeatMetadataProxy = require('../BackbeatMetadataProxy');
const { createBackbeatClient, createS3Client } = require('./utils');
const { authTypeAssumeRole } = require('../constants');

// TODO: test inactive credential deletion
const DELETE_INACTIVE_CREDENTIALS_INTERVAL = 1000 * 60 * 30; // 30m
const MAX_INACTIVE_DURATION = 1000 * 60 * 60 * 2; // 2hr


class ClientManager {
    /**
     * @constructor
     *
     * @param {Object} params -
     * @param {String} params.id -
     * @param {Object} params.authConfig -
     * @param {Object} params.s3Config -
     * @param {String} params.transport -
     * @param {Object} logger -
     */
    constructor(params, logger) {
        this._log = logger;
        this._id = params.id;
        this._authConfig = params.authConfig;
        this._s3Config = params.s3Config;
        this._transport = params.transport || 'http';

        // global variables
        if (this._transport === 'https') {
            this.s3Agent = new https.Agent({ keepAlive: true });
            this.stsAgent = new https.Agent({ keepAlive: true });
        } else {
            this.s3Agent = new http.Agent({ keepAlive: true });
            this.stsAgent = new http.Agent({ keepAlive: true });
        }

        this._stsConfig = null;
        this.s3Clients = {};
        this.backbeatClients = {};
        this.credentialsManager = new CredentialsManager(this._id, this._log);
    }

    initSTSConfig() {
        if (this._authConfig.type === authTypeAssumeRole) {
            const { sts } = this._authConfig;
            const stsWithCreds = this.credentialsManager.resolveExternalFileSync(sts);
            this._stsConfig = {
                endpoint: `${this._transport}://${sts.host}:${sts.port}`,
                credentials: {
                    accessKeyId: stsWithCreds.accessKey,
                    secretAccessKey: stsWithCreds.secretKey,
                },
                region: 'us-east-1',
                signatureVersion: 'v4',
                sslEnabled: this._transport === 'https',
                httpOptions: { agent: this.stsAgent, timeout: 0 },
                maxRetries: 0,
            };
        }
    }

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
     * Return an S3 client instance
     * @param {String} accountId - The account ID of the bucket owner .
     * @return {AWS.S3} The S3 client instance to make requests with
     */
    getS3Client(accountId) {
        const credentials = this.credentialsManager.getCredentials({
            id: accountId,
            accountId,
            stsConfig: this._stsConfig,
            authConfig: this._authConfig,
        });

        if (credentials === null) {
            return null;
        }

        const client = this.s3Clients[accountId];

        if (client) {
            return client;
        }

        this.s3Clients[accountId] = createS3Client({
            transport: this._transport,
            port: this._s3Config.port,
            host: this._s3Config.host,
            credentials,
            agent: this.s3Agent,
        });

        return this.s3Clients[accountId];
    }


    /**
     * Return an backbeat metadata proxy
     * @param {String} accountId - The account ID of the bucket owner .
     * @return {BackbeatMetadataProxy} The S3 client instance to make requests with
     */
    getBackbeatMetadataProxy(accountId) {
        const client = this.getBackbeatClient(accountId);

        if (client === null) {
            return null;
        }

        return new BackbeatMetadataProxy(
        `${this._transport}://${this._s3Config.host}:${this._s3Config.port}`, this._authConfig)
        .setBackbeatClient(client);
    }

    /**
     * Return an backbeat client instance
     * @param {String} accountId - The account ID of the bucket owner .
     * @return {BackbeatClient} The S3 client instance to make requests with
     */
    getBackbeatClient(accountId) {
        const credentials = this.credentialsManager.getCredentials({
            id: accountId,
            accountId,
            stsConfig: this._stsConfig,
            authConfig: this._authConfig,
        });

        if (credentials === null) {
            return null;
        }

        const client = this.backbeatClients[accountId];

        if (client) {
            return client;
        }

        this.backbeatClients[accountId] = createBackbeatClient({
            transport: this._transport,
            port: this._s3Config.port,
            host: this._s3Config.host,
            credentials,
            agent: this.s3Agent,
        });

        return this.backbeatClients[accountId];
    }
}

module.exports = ClientManager;
