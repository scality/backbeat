const http = require('http');
const assert = require('assert');
const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const { getAccountCredentials } =
    require('../../../lib/credentials/AccountCredentials');

const backbeatClientTestPort = 9004;
const mockLogs = {
    info: {
        start: 1,
        cseq: 53,
        prune: 1,
    },
    log: [
        {
            db: 'metastore',
            entries: [
                {
                    key: 'repd/0',
                    value: 'value-1',
                },
            ],
            method: 3,
        },
        {
            db: 'metastore',
            entries: [
                {
                    key: 'repd/0',
                    value: 'value-2',
                },
            ],
            method: 3,
        },
    ],
};

const accountCreds = getAccountCredentials({
    type: 'account',
    account: 'bart',
});

const backbeatClient = new BackbeatClient({
    endpoint: `http://localhost:${backbeatClientTestPort}`,
    sslEnabled: false,
    credentials: accountCreds,
});

class ServerMock {
    onRequest(req, res) {
        if (req.method !== 'GET') {
            res.writeHead(501);
            return res.end(JSON.stringify({
                error: 'mock server only supports GET requests',
            }));
        }
        if (/\/_\/metadata\/admin\/raft_sessions\/[1-8]\/log/.test(req.url)) {
            return res.end(JSON.stringify(mockLogs));
        }
        return res.end(JSON.stringify({ error: 'invalid path' }));
    }
}

const serverMock = new ServerMock();

describe('BackbeatClient unit tests with mock server', () => {
    let httpServer;
    before(done => {
        httpServer = http.createServer(
            (req, res) => serverMock.onRequest(req, res))
                .listen(backbeatClientTestPort, done);
    });

    after(() => httpServer.close());

    it('should get raftLogs', done => {
        const destReq = backbeatClient.getRaftLog({
            LogId: '1',
        });
        return destReq.send((err, data) => {
            assert.ifError(err);
            assert.deepStrictEqual(data, mockLogs);
            return done();
        });
    });
});
