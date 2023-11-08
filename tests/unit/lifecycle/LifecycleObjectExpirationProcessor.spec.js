const assert = require('assert');
const config = require('../../config.json');
const LifecycleObjectExpirationProcessor =
    require('../../../extensions/lifecycle/objectProcessor/LifecycleObjectExpirationProcessor');

describe('LifecycleObjectExpirationProcessor', () => {
    let objectProcessor;

    beforeEach(() => {
        objectProcessor = new LifecycleObjectExpirationProcessor(
            config.zookeeper,
            config.kafka,
            config.extensions.lifecycle,
            config.s3,
        );
    });

    it('should contain object tasks topic in consumer params', () => {
        const consumerParams = objectProcessor.getConsumerParams();
        assert.deepStrictEqual(Object.keys(consumerParams), [config.extensions.lifecycle.objectTasksTopic]);
        assert.strictEqual(
            consumerParams[config.extensions.lifecycle.objectTasksTopic].topic,
            config.extensions.lifecycle.objectTasksTopic,
        );
    });
});
