const assert = require('assert');
const { startProbeServer, getProbeConfig } =
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

describe.only('getProbeConfig', function() {
    it('returns the probeServer config when siteNames is empty and probeServer is a single object', function() {
      const queueProcessorConfig = {
        probeServer: { bindAddress: '127.0.0.1', port: '8080' }
      };
      const siteNames = [];
  
      const result = getProbeConfig(queueProcessorConfig, siteNames);
      assert.deepStrictEqual(result, { bindAddress: '127.0.0.1', port: '8080' });
    });
  
    it('returns undefined when siteNames is empty and probeServer is not a single object', function() {
      const queueProcessorConfig = {
        probeServer: [{ site: 'site1', bindAddress: '127.0.0.1', port: '8080' }]
      };
      const siteNames = [];
  
      const result = getProbeConfig(queueProcessorConfig, siteNames);
      assert.strictEqual(result, undefined);
    });
  
    it('returns the correct site config when probeServer is an array and siteNames has one matching element', function() {
      const queueProcessorConfig = {
        probeServer: [
          { site: 'site1', bindAddress: '127.0.0.1', port: '8080' },
          { site: 'site2', bindAddress: '127.0.0.2', port: '8081' }
        ]
      };
      const siteNames = ['site2'];
  
      const result = getProbeConfig(queueProcessorConfig, siteNames);
      assert.deepStrictEqual(result, { site: 'site2', bindAddress: '127.0.0.2', port: '8081' });
    });
  
    it('returns undefined when probeServer is an array and siteNames has no matching element', function() {
      const queueProcessorConfig = {
        probeServer: [
          { site: 'site1', bindAddress: '127.0.0.1', port: '8080' }
        ]
      };
      const siteNames = ['site2'];
  
      const result = getProbeConfig(queueProcessorConfig, siteNames);
      assert.strictEqual(result, undefined);
    });
  });
