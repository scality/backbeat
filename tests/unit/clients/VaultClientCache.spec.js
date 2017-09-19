const assert = require('assert');

const VaultClientCache = require('../../../lib/clients/VaultClientCache');

describe('Vault client cache', () => {
    let client;
    let client2;
    let vcc;

    beforeEach(() => {
        vcc = new VaultClientCache();
        vcc.setHost('source:s3', '1.2.3.4');
        vcc.setPort('source:s3', 8500);
        vcc.setHost('dest:s3', '5.6.7.8');
        vcc.setPort('dest:s3', 8500);
    });

    it('should cache client instances', () => {
        client = vcc.getClient('source:s3');
        client2 = vcc.getClient('dest:s3');
        assert.notStrictEqual(client, client2);
        assert.strictEqual(vcc.getClient('source:s3'), client);
        assert.strictEqual(vcc.getClient('dest:s3'), client2);

        client = vcc.getClient('dest:multi', '10.0.0.1', 8443);
        client2 = vcc.getClient('dest:multi', '10.0.0.2', 8443);
        assert.notStrictEqual(client2, client);
        client2 = vcc.getClient('dest:multi', '10.0.0.1', 8443);
        assert.strictEqual(client2, client);
    });

    it('should honor setHost()/setPort()', () => {
        client = vcc.getClient('source:s3');
        assert.strictEqual(client.getServerHost(), '1.2.3.4');
        assert.strictEqual(client.getServerPort(), 8500);

        client2 = vcc.getClient('dest:s3');
        assert.strictEqual(client2.getServerHost(), '5.6.7.8');
        assert.strictEqual(client2.getServerPort(), 8500);

        client = vcc.getClient('source:s3', '42.42.42.42', 1234);
        // setHost() has precedence
        assert.strictEqual(client.getServerHost(), '1.2.3.4');
        assert.strictEqual(client.getServerPort(), 8500);
    });

    it('should use host/port provided to getClient()', () => {
        client = vcc.getClient('dest:multi', '10.0.0.1', 8443);
        assert.strictEqual(client.getServerHost(), '10.0.0.1');
        assert.strictEqual(client.getServerPort(), 8443);

        client2 = vcc.getClient('dest:multi', '10.0.0.2', 8443);
        assert.strictEqual(client2.getServerHost(), '10.0.0.2');
        assert.strictEqual(client2.getServerPort(), 8443);
    });
});
