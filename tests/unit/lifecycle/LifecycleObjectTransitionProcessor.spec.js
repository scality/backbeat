const assert = require('assert');
const config = require('../../config.json');
const LifecycleObjectTransitionProcessor =
    require('../../../extensions/lifecycle/objectProcessor/LifecycleObjectTransitionProcessor');

describe('LifecycleObjectExpirationProcessor', () => {
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
});
