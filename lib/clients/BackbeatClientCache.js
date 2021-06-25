const BackbeatClient = require('./BackbeatClient');
// TODO: move BackbeatMetadataProxy
const BackbeatMetadataProxy = require('../BackbeatMetadataProxy');
const ClientCache = require('./BaseClientCache');

class BackbeatClientCache extends ClientCache {
    constructor(logger) {
        super();
        this._logger = logger;
    }

    getClient(profileName) {
        if (this._profiles[profileName] === undefined) {
            this._logger.debug('empty profile', {
                method: 'BackbeatClientCache::getClient',
                profileName,
            });
            return null;
        }

        if (this._clients[profileName] === undefined) {
            const profile = this.getValidProfile(profileName);

            if (profile.error) {
                this._logger.error('invalid client profile', {
                    method: 'BackbeatClientCache::getClient',
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

            const { transport, host, port, credentials, agent } = profile;
            const endpoint = `${transport}://${host}:${port}`;
            const client = new BackbeatClient({
                endpoint,
                credentials,
                sslEnabled: transport === 'https',
                httpOptions: { agent, timeout: 0 },
                maxRetries: 0,
            });
            // NOTE: does not support role auth type
            this._clients[profileName] = new BackbeatMetadataProxy(
                null, agent).setBackbeatClient(client);
        }

        return this._clients[profileName];
    }
}

module.exports = BackbeatClientCache;
