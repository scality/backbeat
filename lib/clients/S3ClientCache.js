const AWS = require('aws-sdk');

const ClientCache = require('./BaseClientCache');

class S3ClientCache extends ClientCache {
    constructor(logger) {
        super();
        this._logger = logger;
    }

    getClient(profileName) {
        if (this._profiles[profileName] === undefined) {
            this._logger.debug('empty profile', {
                method: 's3ClientCache::getClient',
                profileName,
            });
            return null;
        }

        if (this._clients[profileName] === undefined) {
            const profile = this.getValidProfile(profileName);

            if (profile.error) {
                this._logger.error('invalid client profile', {
                    method: 's3ClientCache::getClient',
                    error: profile.error,
                    profileName,
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

            const { transport, host, port, credentials, agent } = profile;
            this._clients[profileName] = new AWS.S3({
                endpoint: `${transport}://${host}:${port}`,
                credentials,
                sslEnabled: transport === 'https',
                s3ForcePathStyle: true,
                signatureVersion: 'v4',
                httpOptions: { agent, timeout: 0 },
                maxRetries: 0,
            });
        }

        return this._clients[profileName];
    }
}

module.exports = S3ClientCache;
