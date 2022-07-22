const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');

const config = require('../../../lib/Config');
const { coldStorageRestoreTopicPrefix } = config.extensions.lifecycle;

const LifecycleQueuePopulator = require('../../../extensions/lifecycle/LifecycleQueuePopulator');

describe('LifecycleQueuePopulator', () => {
    function _stubSetupProducer(topic, cb) {
        // fake producer connection
        setTimeout(() => {
            this._producers[topic] = {
                send: () => {},
            };
            return cb();
        }, 100);
    }

    describe('Producer', () => {
        let lcqp;
        const params = {
            authConfig: {
                transport: 'http',
            },
            logger: new werelogs.Logger('test:LifecycleQueuePopulator'),
        };
        const coldLocationConfigs = {
            'dmf-v1': {
                isCold: true,
                type: 'dmf',
            },
            'dmf-v2': {
                isCold: true,
                type: 'dmf',
            },
        };
        const locationConfigs = {
            'us-east-1': {
                type: 'aws_s3',
            },
            'us-east-2': {
                type: 'aws_s3',
            },
        };

        beforeEach(() => {
            lcqp = new LifecycleQueuePopulator(params);
            sinon.stub(lcqp, '_setupProducer').callsFake(_stubSetupProducer);
        });
        afterEach(() => {
            sinon.restore();
        });
        it('should not setup producers if no cold locations are configured', done => {
            lcqp.locationConfigs = locationConfigs;
            lcqp.setupProducers(() => {
                const producers = Object.keys(lcqp._producers);
                assert.strictEqual(producers.length, 0);
                done();
            });
        });
        it('should have one producer per cold location', done => {
            lcqp.locationConfigs = Object.assign({}, locationConfigs, coldLocationConfigs);
            lcqp.setupProducers(() => {
                const producers = Object.keys(lcqp._producers);
                const coldLocations = Object.keys(coldLocationConfigs);
                assert.strictEqual(producers.length, coldLocations.length);
                coldLocations.forEach(loc => {
                    const topic = `${coldStorageRestoreTopicPrefix}${loc}`;
                    assert(producers.includes(topic));
                });
                done();
            });
        });
    });
});
