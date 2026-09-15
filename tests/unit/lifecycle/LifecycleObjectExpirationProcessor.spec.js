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

    it('should not pin fromOffset on the object tasks topic', () => {
        const consumerParams = objectProcessor.getConsumerParams();
        assert.strictEqual(
            consumerParams[config.extensions.lifecycle.objectTasksTopic].fromOffset,
            undefined,
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

        it('should close the client manager once the consumers are closed', done => {
            const spy = sinon.spy(objectProcessor.clientManager, 'close');
            const consumersClosed = sinon.stub();
            objectProcessor._consumers = {
                close: cb => {
                    consumersClosed();
                    cb();
                },
            };
            objectProcessor.close(err => {
                assert.ifError(err);
                assert(spy.calledOnce);
                assert(spy.calledAfter(consumersClosed));
                spy.restore();
                done();
            });
        });
    });
});
