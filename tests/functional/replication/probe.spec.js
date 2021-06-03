const assert = require('assert');
const sinon = require('sinon');
const { startProbeServer } =
    require('../../../extensions/replication/queueProcessor/Probe');
const { DEFAULT_LIVE_ROUTE } = require('arsenal').network.probe.ProbeServer;
const http = require('http');

/** Return a mock queue processor for testing
*/
function mockQueueProcessor() {
    return {
        site: {
            handleLiveness: sinon.stub(),
        }
    }
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
        startProbeServer(mockQp, config, probeServer => {
            assert.strictEqual(probeServer, undefined);
            done();
        });
    });

    it('is not created with no config', done => {
        process.env.CRR_METRICS_PROBE = 'true';
        const mockQp = mockQueueProcessor();
        const config = undefined;
        startProbeServer(mockQp, config, probeServer => {
            assert.strictEqual(probeServer, undefined);
            done();
        });
    });

    it('creates probe server for liveness', done => {
        process.env.CRR_METRICS_PROBE = 'true';
        const mockQp = mockQueueProcessor();
        // return sample error for liveness
        mockQp.site.handleLiveness.returns('error msg');
        const config = {
            bindAddress: 'localhost',
            port: 52555,
        };
        startProbeServer(mockQp, config, probeServer => {
            probeServer.onStop(done);
            http.get('http://localhost:52555' + DEFAULT_LIVE_ROUTE, res => {
                assert.strictEqual(500, res.statusCode);

                let rawData = [];
                res.on('data', chunk => {
                    rawData.push(chunk);
                });
                res.on('end', () => {
                    data = JSON.parse(rawData.join(''));
                    assert.strictEqual(data.errorMessage, '["error msg"]');
                    probeServer.stop();
                });
            });
        });
    })
});
