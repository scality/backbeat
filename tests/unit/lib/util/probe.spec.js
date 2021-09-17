const assert = require('assert');
const { startProbeServer } =
    require('../../../../lib/util/probe');

describe('Probe server', () => {
    it('is not created with no config', done => {
        const config = undefined;
        startProbeServer(config, (err, probeServer) => {
            assert(err);
            assert.strictEqual(probeServer, undefined);
            done();
        });
    });

    it('calls back with error if one occurred', done => {
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
