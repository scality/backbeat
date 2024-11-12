'use strict'; // eslint-disable-line

const assert = require('assert');
const sinon = require('sinon');
const { EventEmitter } = require('events');
const MongoQueueProcessor = require('../../../extensions/mongoProcessor/MongoQueueProcessor');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');

describe('MongoQueueProcessor', () => {
    let mqp;

    beforeEach(() => {
        mqp = new MongoQueueProcessor(
            { hosts: 'localhost:9092', site: 'test-site' },
            { topic: 'test-topic', groupId: 'test-group' },
            { logName: 's3-recordlog', replicaSetHosts: 'localhost:27018' },
            {}
        );
        // eslint-disable-next-line no-console
        console.log('mqp', mqp.logger);
        // eslint-disable-next-line no-console
        console.log('mqp', mqp);
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('::start', () => {
        it('should log an error and exit if MongoDB connection fails', done => {
            const loggerErrorStub = sinon.stub(mqp.logger, 'error');
            const loggerFatalStub = sinon.stub(mqp.logger, 'fatal');
            const processExitStub = sinon.stub(process, 'exit');

            sinon.stub(mqp._mongoClient, 'setup').callsFake(callback => {
                callback(new Error('Simulated connection failure'));
            });

            mqp.start();

            setTimeout(() => {
                assert(loggerErrorStub.calledOnce);
                assert(loggerErrorStub.calledWith('could not connect to MongoDB'));
                assert(loggerFatalStub.calledOnce);
                assert(loggerFatalStub.calledWith('error starting mongo queue processor'));
                assert(processExitStub.calledOnce);
                assert(processExitStub.calledWith(1));
                done();
            }, 100);
        });

        it.skip('should initialize and start the Kafka consumer', done => {
            const consumerStub = sinon.stub(BackbeatConsumer.prototype, 'on');
            const subscribeStub = sinon.stub(BackbeatConsumer.prototype, 'subscribe');

            sinon.stub(mqp._mongoClient, 'setup').callsFake(callback => {
                callback(null);
            });

            mqp.start();

            setTimeout(() => {
                assert(consumerStub.calledTwice);
                assert(subscribeStub.calledOnce);
                done();
            }, 100);
        });
    });

    describe('::stop', () => {
        it('should close the Kafka consumer if it exists', done => {
            mqp._consumer = new EventEmitter();
            mqp._consumer.close = sinon.stub().callsFake(callback => {
                callback();
            });

            const loggerDebugStub = sinon.stub(mqp.logger, 'debug');

            mqp.stop(() => {
                assert(loggerDebugStub.calledOnce);
                assert(loggerDebugStub.calledWith('closing kafka consumer'));
                assert(mqp._consumer.close.calledOnce);
                done();
            });
        });

        it('should log a message if there is no Kafka consumer to close', done => {
            const loggerDebugStub = sinon.stub(mqp.logger, 'debug');

            mqp.stop(() => {
                assert(loggerDebugStub.calledOnce);
                assert(loggerDebugStub.calledWith('no kafka consumer to close'));
                done();
            });
        });
    });
});
