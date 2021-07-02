const assert = require('assert');
const { Logger } = require('werelogs');

const { ClientCache } = require('../../../lib/clients/ClientCache');

function createStubClient(profile) {
    return Object.assign({}, profile);
}

const testProfile1 = 'test-profile-1';
const testProfile2 = 'test-profile-2';

describe('ClientCache', () => {
    const log = new Logger('test:ClientCache');

    it('should correct set properties', () => {
        const cc = new ClientCache(createStubClient);
        cc.setHost(testProfile1, '1.1.1.1')
            .setPort(testProfile1, 1111)
            .setTransport(testProfile1, 'transport1')
            .setAgent(testProfile1, 'agent1')
            .setCredentials(testProfile1, 'creds1');

        cc.setHost(testProfile2, '2.2.2.2')
            .setPort(testProfile2, 2222)
            .setTransport(testProfile2, 'transport2')
            .setAgent(testProfile2, 'agent2')
            .setCredentials(testProfile2, 'creds2');

        assert.deepStrictEqual(cc._profiles[testProfile1], {
            host: '1.1.1.1', port: 1111,
            transport: 'transport1', agent: 'agent1',
            credentials: 'creds1',
        });
        assert.deepStrictEqual(cc._profiles[testProfile2], {
            host: '2.2.2.2', port: 2222,
            transport: 'transport2', agent: 'agent2',
            credentials: 'creds2',
        });
        assert.notEqual(cc._profiles[testProfile1], cc._profiles[testProfile2]);
    });

    describe('::getClient', () => {
        it('should return null if client create function is not set', () => {
            const cc = new ClientCache();
            cc.setHost(testProfile1, '1.1.1.1')
                .setPort(testProfile1, 1111);
            assert.strictEqual(cc.getClient(testProfile1, log), null);
        });

        it('should return null if client create function is invalid', () => {
            const cc = new ClientCache('notafunction');
            cc.setHost(testProfile1, '1.1.1.1')
                .setPort(testProfile1, 1111);
            assert.strictEqual(cc.getClient(testProfile1, log), null);
        });

        it('should return null if profile does not exist', () => {
            const cc = new ClientCache(createStubClient);
            assert.strictEqual(cc._profiles[testProfile1], undefined);
            assert.strictEqual(cc.getClient(testProfile1, log), null);
        });

        it('should create a new client if cached client does not exist', () => {
            const cc = new ClientCache(createStubClient);
            cc.setHost(testProfile1, '1.1.1.1')
                .setPort(testProfile1, 1111);
            assert.strictEqual(cc._clients[testProfile1], undefined);
            const c1 = cc.getClient(testProfile1, log);
            assert.strictEqual(cc._clients[testProfile1], c1);
        });

        it('should return the correct cached client', () => {
            const cc = new ClientCache(createStubClient);
            cc.setHost(testProfile1, '1.1.1.1')
                .setPort(testProfile1, 1111);
            cc.setHost(testProfile2, '2.2.2.2')
                .setPort(testProfile2, 2222);

            const c1 = cc.getClient(testProfile1, log);
            const c2 = cc.getClient(testProfile1, log);
            assert.strictEqual(c1, c2);
            assert.deepStrictEqual(c1, c2);

            const other = cc.getClient(testProfile2, log);
            assert.notDeepStrictEqual(c1, other);
        });

        it('should not cache an invalid client(null)', () => {
            const cc = new ClientCache(() => null);
            cc.setHost(testProfile1, '1.1.1.1')
                .setPort(testProfile1, 1111);
            assert.strictEqual(cc._clients[testProfile1], undefined);
            const c1 = cc.getClient(testProfile1, log);
            assert.strictEqual(c1, null);
            assert.strictEqual(cc._clients[testProfile1], undefined);
        });
    });

    describe('::deleteProfile', () => {
        it('should delete profile', () => {
            const cc = new ClientCache(createStubClient);
            cc.setHost(testProfile1, '1.1.1.1')
                .setPort(testProfile1, 1111);
            assert(cc._profiles[testProfile1]);
            cc.deleteProfile(testProfile1);
            assert(!cc._profiles[testProfile1]);
        });

        it('should delete client', () => {
            const cc = new ClientCache(createStubClient);
            cc.setHost(testProfile1, '1.1.1.1')
                .setPort(testProfile1, 1111);
            cc.getClient(testProfile1, log);
            assert(cc._clients[testProfile1]);
            cc.deleteProfile(testProfile1);
            assert(!cc._clients[testProfile1]);
        });
    });
});

