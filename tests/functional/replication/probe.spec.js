const assert = require('assert');
const sinon = require('sinon');
const { startProbeServer, getProbeConfig } =
    require('../../../extensions/replication/queueProcessor/Probe');
const { DEFAULT_LIVE_ROUTE } = require('arsenal').network.probe.ProbeServer;
const http = require('http');

/**
 * @returns {Object} Mock queue processor
*/
function mockQueueProcessor() {
    return {
        handleLiveness: sinon.stub(),
    };
}

describe('Probe server', () => {
    afterEach(() => {
        // reset any possible env var set
        delete process.env.CRR_METRICS_PROBE;
    });

    it('is not created when disabled', done => {
        process.env.CRR_METRICS_PROBE = 'false';
        const mockQp = mockQueueProcessor();
        const config = {
            bindAddress: 'localhost',
            port: 52555,
        };
        startProbeServer(mockQp, config, (err, probeServer) => {
            assert.ifError(err);
            assert.strictEqual(probeServer, undefined);
            done();
        });
    });

    it('is not created with no config', done => {
        process.env.CRR_METRICS_PROBE = 'true';
        const mockQp = mockQueueProcessor();
        const config = undefined;
        startProbeServer(mockQp, config, (err, probeServer) => {
            assert.ifError(err);
            assert.strictEqual(probeServer, undefined);
            done();
        });
    });

    it('calls back with error if one occurred', done => {
        process.env.CRR_METRICS_PROBE = 'true';
        const mockQp = mockQueueProcessor();
        const config = {
            bindAddress: 'httppp://badaddress',
            // inject an error with a bad port
            port: 52525,
        };
        startProbeServer(mockQp, config, (err, probeServer) => {
            assert.notStrictEqual(err, undefined);
            assert.strictEqual(probeServer, undefined);
            done();
        });
    });

    it('creates probe server for liveness', done => {
        process.env.CRR_METRICS_PROBE = 'true';
        const mockQp = mockQueueProcessor();
        // return sample error for liveness
        mockQp.handleLiveness.returns('error msg');
        const config = {
            bindAddress: 'localhost',
            port: 52555,
        };
        startProbeServer(mockQp, config, (err, probeServer) => {
            assert.ifError(err);
            probeServer.onStop(done);
            http.get(`http://localhost:52555${DEFAULT_LIVE_ROUTE}`, res => {
                assert.strictEqual(res.statusCode, 500);

                const rawData = [];
                res.on('data', chunk => {
                    rawData.push(chunk);
                });
                res.on('end', () => {
                    const data = JSON.parse(rawData.join(''));
                    assert.strictEqual(data.errorMessage, 'error msg');
                    probeServer.stop();
                });
            });
        });
    });

    it('can get config', () => {
        const qpConfig = {
            probeServer: [
                {
                    site: 'sf',
                    bindAddress: 'localhost',
                    port: '4043',
                }, {
                    site: 'us-east-1',
                    bindAddress: 'localhost',
                    port: '4044',
                },
            ],
        };
        const cfg = getProbeConfig(qpConfig, 'us-east-1');
        assert.deepStrictEqual(cfg, {
            site: 'us-east-1',
            bindAddress: 'localhost',
            port: '4044',
        });
    });

    it('returns undefined if site not listed', () => {
        const qpConfig = {
            probeServer: [
                {
                    site: 'sf',
                    bindAddress: 'localhost',
                    port: '4043',
                },
            ],
        };
        const cfg = getProbeConfig(qpConfig, 'missing site');
        assert.deepStrictEqual(cfg, undefined);
    });

    it('returns undefined if no sites', () => {
        const qpConfig = {
            probeServer: [],
        };
        const cfg = getProbeConfig(qpConfig, 'no configs');
        assert.deepStrictEqual(cfg, undefined);
    });

    it('returns undefined if no probe server config', () => {
        const qpConfig = {};
        const cfg = getProbeConfig(qpConfig, 'no server configs');
        assert.deepStrictEqual(cfg, undefined);
    });

    it('returns undefined if no config at all', () => {
        const cfg = getProbeConfig(undefined, 'no server configs');
        assert.deepStrictEqual(cfg, undefined);
    });
});
