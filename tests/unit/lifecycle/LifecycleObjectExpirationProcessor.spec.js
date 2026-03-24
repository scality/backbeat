const assert = require('assert');
const sinon = require('sinon');
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

    describe('close() expiration processor', () => {
        it('should call close on consumers when they exist', done => {
            let closeCalled = false;
            objectProcessor._consumers = {
                close: cb => {
                    closeCalled = true;
                    cb();
                },
            };
            objectProcessor.close(err => {
                assert.ifError(err);
                assert.strictEqual(closeCalled, true);
                done();
            });
        });

        it('should call callback immediately when consumers is null', done => {
            assert.strictEqual(objectProcessor._consumers, null);
            objectProcessor.close(err => {
                assert.ifError(err);
                done();
            });
        });

        it('should clear deleteInactiveCredentialsInterval if set', done => {
            const spy = sinon.spy(global, 'clearInterval');
            const interval = setInterval(() => {}, 100000);
            objectProcessor._deleteInactiveCredentialsInterval = interval;
            objectProcessor.close(err => {
                assert.ifError(err);
                assert(spy.calledWith(interval));
                spy.restore();
                done();
            });
        });
    });
});
