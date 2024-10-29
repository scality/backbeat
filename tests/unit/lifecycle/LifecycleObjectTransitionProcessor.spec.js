const assert = require('assert');
const config = require('../../config.json');
const LifecycleObjectTransitionProcessor =
    require('../../../extensions/lifecycle/objectProcessor/LifecycleObjectTransitionProcessor');

describe('LifecycleObjectTransitionProcessor', () => {
    let objectProcessor;

    beforeEach(() => {
        objectProcessor = new LifecycleObjectTransitionProcessor(
            config.zookeeper,
            config.kafka,
            config.extensions.lifecycle,
            config.s3,
        );
    });

    it('should contain transition tasks topic in consumer params', () => {
        const consumerParams = objectProcessor.getConsumerParams();
        assert.deepStrictEqual(Object.keys(consumerParams), [config.extensions.lifecycle.transitionTasksTopic]);
        assert.strictEqual(
            consumerParams[config.extensions.lifecycle.transitionTasksTopic].topic,
            config.extensions.lifecycle.transitionTasksTopic
        );
    });

    it('should set up gcProducer when start is called', done => {
        objectProcessor.start(err => {
            assert.ifError(err);
            assert(objectProcessor._gcProducer, 'gcProducer should be set');
            done();
        });
    });
});
