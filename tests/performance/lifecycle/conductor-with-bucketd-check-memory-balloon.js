const baseConfig = require('../../../lib/Config');
const LifecycleConductor = require('../../../extensions/lifecycle/conductor/LifecycleConductor');

const lcConfig = {
    ...baseConfig.extensions.lifecycle,
    auth: {
        type: '',
    },
    conductor: {
        cronRule: '12 12 12 12 12',
        concurrency: 10000,
        bucketSource: 'bucketd',
        bucketd: {
            host: '127.0.0.1',
            port: 9001,
        },
        backlogControl: {
            enabled: true,
        },
    },
};

const lc = new LifecycleConductor(
    baseConfig.zookeeper,
    baseConfig.kafka,
    lcConfig,
    baseConfig.extensions.replication
);

describe('Lifecycle Conductor', function testBackpressure() {
    this.timeout(10 * 60 * 1000);

    it('should apply backpressure on bucket queue instead of ballooning', done => {
        lc.init(() => {
            lc.processBuckets(err => {
                lc.stop();
                done(err);
            });
        });
    });
});
