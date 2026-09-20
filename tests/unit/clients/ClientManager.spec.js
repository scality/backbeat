const assert = require('assert');
const sinon = require('sinon');

const ClientManager = require('../../../lib/clients/ClientManager');

const fakeLogger = require('../../utils/fakeLogger');

function createClientManager() {
    return new ClientManager({
        id: 'test-extension',
        authConfig: { type: 'account', account: 'bart' },
        s3Config: { host: 's3.zenko.local', port: 80 },
        transport: 'http',
    }, fakeLogger);
}

describe('ClientManager', () => {
    let clientManager;

    beforeEach(() => {
        clientManager = createClientManager();
    });

    afterEach(() => {
        clientManager.close();
    });

    describe('initCredentialsManager', () => {
        it('should not let the credentials sweep hold the event loop open', () => {
            clientManager.initCredentialsManager();

            assert.strictEqual(
                clientManager._deleteInactiveCredentialsInterval.hasRef(), false);
        });

        it('should drop the clients of credentials that got removed', () => {
            clientManager.initCredentialsManager();
            clientManager.s3Clients['123456789012'] = {};
            clientManager.backbeatClients['123456789012'] = {};

            clientManager.credentialsManager.emit('deleteCredentials', '123456789012');

            assert.deepStrictEqual(clientManager.s3Clients, {});
            assert.deepStrictEqual(clientManager.backbeatClients, {});
        });
    });

    describe('idle detection', () => {
        let clock;

        beforeEach(() => {
            clock = sinon.useFakeTimers();
        });

        afterEach(() => {
            clock.restore();
            sinon.restore();
        });

        function sweep() {
            clock.tick(1000 * 60 * 30);
        }

        it('should report itself idle once the sweep left nothing behind', () => {
            const idle = sinon.stub();
            clientManager.initCredentialsManager();
            clientManager.on('idle', idle);

            sweep();

            assert(idle.calledOnce);
        });

        it('should stay silent while credentials are still cached', () => {
            const idle = sinon.stub();
            sinon.stub(clientManager.credentialsManager, 'removeInactiveCredentials')
                .returns(1);
            clientManager.initCredentialsManager();
            clientManager.on('idle', idle);

            sweep();

            assert(idle.notCalled);
        });

        it('should stay silent while clients are still held', () => {
            const idle = sinon.stub();
            clientManager.initCredentialsManager();
            clientManager.on('idle', idle);
            clientManager.backbeatClients['123456789012'] = {};

            sweep();

            assert(idle.notCalled);
        });

        it('should stop reporting once closed', () => {
            const idle = sinon.stub();
            clientManager.initCredentialsManager();
            clientManager.on('idle', idle);

            clientManager.close();
            sweep();

            assert(idle.notCalled);
        });
    });

    describe('close', () => {
        it('should clear the credentials sweep', () => {
            clientManager.initCredentialsManager();

            clientManager.close();

            assert.strictEqual(clientManager._deleteInactiveCredentialsInterval, null);
        });

        it('should stop listening for credentials removal', () => {
            clientManager.initCredentialsManager();

            clientManager.close();

            assert.strictEqual(
                clientManager.credentialsManager.listenerCount('deleteCredentials'), 0);
        });

        it('should do nothing when initCredentialsManager was never called', () => {
            assert.doesNotThrow(() => clientManager.close());
            assert.strictEqual(clientManager._deleteInactiveCredentialsInterval, null);
        });

        it('should be safe to call twice', () => {
            clientManager.initCredentialsManager();

            clientManager.close();
            assert.doesNotThrow(() => clientManager.close());
        });

        it('should drop the idle sockets of its agents', () => {
            const s3Agent = sinon.spy(clientManager.s3Agent, 'destroy');
            const stsAgent = sinon.spy(clientManager.stsAgent, 'destroy');

            clientManager.close();

            assert(s3Agent.calledOnce);
            assert(stsAgent.calledOnce);
            sinon.restore();
        });
    });
});
