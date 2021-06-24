const assert = require('assert');
const { startProbeServer } =
    require('../../../../lib/util/probe');

describe('Probe server', () => {
    afterEach(() => {
        // reset any possible env var set
        delete process.env.CRR_METRICS_PROBE;
    });

    it('is not created when disabled', done => {
        process.env.CRR_METRICS_PROBE = 'false';
        const config = {
            bindAddress: 'localhost',
            port: 52555,
        };
        startProbeServer(config, (err, probeServer) => {
            assert.ifError(err);
            assert.strictEqual(probeServer, undefined);
            done();
        });
    });

    it('is not created with no config', done => {
        process.env.CRR_METRICS_PROBE = 'true';
        const config = undefined;
        startProbeServer(config, (err, probeServer) => {
            assert.ifError(err);
            assert.strictEqual(probeServer, undefined);
            done();
        });
    });

    it('calls back with error if one occurred', done => {
        process.env.CRR_METRICS_PROBE = 'true';
        const config = {
            bindAddress: 'httppp://badaddress',
            // inject an error with a bad port
            port: 52525,
        };
        startProbeServer(config, (err, probeServer) => {
            assert.notStrictEqual(err, undefined);
            assert.strictEqual(probeServer, undefined);
            done();
        });
    });
});
