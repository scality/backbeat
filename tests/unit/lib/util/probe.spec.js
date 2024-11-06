const assert = require('assert');
const { startProbeServer, getReplicationProbeConfig } =
    require('../../../../lib/util/probe');
const Logger = require('werelogs').Logger;

describe('Probe server', () => {
    it('should return undefined when no config is passed', done => {
        const config = undefined;
        startProbeServer(config, (err, probeServer) => {
            assert.ifError(err);
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

describe('getReplicationProbeConfig', () => {
  const log = new Logger('getReplicationProbeConfig');
    it('returns the probeServer config when siteNames is empty and probeServer is a single object', () => {
      const config = {
        queueProcessor: {
          probeServer: { bindAddress: '127.0.0.1', port: '8080' },
        },
      };
      const siteNames = [];

      const result = getReplicationProbeConfig(config, siteNames, log);
      assert.deepStrictEqual(result, { bindAddress: '127.0.0.1', port: '8080' });
    });

    it('returns undefined when siteNames is empty and probeServer is not a single object', () => {
      const config = {
        queueProcessor: {
          probeServer: [{ site: 'site1', bindAddress: '127.0.0.1', port: '8080' }],
        },
      };
      const siteNames = [];

      const result = getReplicationProbeConfig(config, siteNames, undefined, log);
      assert.strictEqual(result, undefined);
    });

    it('returns the correct site config when probeServer is an array and siteNames has one matching element', () => {
      const config = {
        queueProcessor: {
          probeServer: [
            { site: 'site1', bindAddress: '127.0.0.1', port: '8080' },
            { site: 'site2', bindAddress: '127.0.0.2', port: '8081' },
          ],
        },
      };
      const siteNames = ['site2'];

      const result = getReplicationProbeConfig(config, siteNames, undefined, log);
      assert.deepStrictEqual(result, config.queueProcessor.probeServer[1]);
    });

    it('returns undefined when probeServer is an array and siteNames has no matching element', () => {
      const config = {
        queueProcessor: {
          probeServer: [
            { site: 'site1', bindAddress: '127.0.0.1', port: '8080' },
          ],
        },
      };
      const siteNames = ['site2'];

      const result = getReplicationProbeConfig(config, siteNames, undefined, log);
      assert.strictEqual(result, undefined);
    });

    it('returns undefined when siteNames contains more than one element', () => {
        const config = {
            queueProcessor: {
              probeServer: [
                { site: 'site1', bindAddress: '127.0.0.1', port: '8080' },
                { site: 'site2', bindAddress: '127.0.0.2', port: '8081' },
              ]
            },
        };
        const siteNames = ['site1', 'site2']; // More than one element in siteNames

        const result = getReplicationProbeConfig(config, siteNames, undefined, log);
        assert.strictEqual(result, undefined);
    });

    it('returns probeserver when probeServer is not an array and siteNames is not empty', () => {
        const config = {
          queueProcessor: {
            probeServer: { bindAddress: '127.0.0.1', port: '8080' } // probeServer is a single object
          },
        };
        const siteNames = ['site1']; // siteNames is not empty

        const result = getReplicationProbeConfig(config, siteNames, undefined, log);
        assert.deepStrictEqual(result, config.queueProcessor.probeServer);
    });

    it('returns replay processor probeServer configuration that matches the topic and site', () => {
      const config = {
        queueProcessor: {
          probeServer: [
            { site: 'site1', bindAddress: '127.0.0.1', port: '8080' },
            { site: 'site2', bindAddress: '127.0.0.2', port: '8081' },
          ],
        },
        replayProcessor: {
          probeServer: [
            { site: 'site1', bindAddress: '127.0.0.1', port: '8080', topicName: 'replay-site1' },
            { site: 'site2', bindAddress: '127.0.0.1', port: '8080', topicName: 'replay-site2' },
          ]
        },
      };
      const siteNames = ['site1'];

      const result = getReplicationProbeConfig(config, siteNames, 'replay-site1', log);
      assert.deepStrictEqual(result, config.replayProcessor.probeServer[0]);
    });
  });
