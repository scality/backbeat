const assert = require('assert');
const sinon = require('sinon');
const errors = require('arsenal').errors;
const werelogs = require('werelogs');
const events = require('events');

const logger = new werelogs.Logger('connect-wrapper-logger');

const ChangeStream =
    require('../../../../lib/wrappers/ChangeStream');

describe('ChangeStream', () => {
    let wrapper;

    beforeEach(() => {
        wrapper = new ChangeStream({
            logger,
            collection: {
                watch: () => new events.EventEmitter(),
            },
            handler: () => {},
            pipeline: [],
            throwOnError: false,
        });
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('constructor', () => {
        it('should fail when params invalid', done => {
            assert.throws(() => new ChangeStream({}));
            assert.throws(() => new ChangeStream({
                logger,
            }));
            assert.throws(() => new ChangeStream({
                logger,
                collection: {},
                handler: () => {},
            }));
            assert.throws(() => new ChangeStream({
                pipeline: [],
                throwOnError: false,
            }));
            assert.throws(() => new ChangeStream({
                logger,
                collection: 'coll',
                handler: () => {},
                pipeline: [],
                throwOnError: false,
            }));
            assert.throws(() => new ChangeStream({
                logger,
                collection: {},
                handler: () => {},
                pipeline: {},
                throwOnError: false,
            }));
            const csw = new ChangeStream({
                logger,
                collection: {},
                handler: () => {},
                pipeline: [],
                throwOnError: false,
            });
            assert(csw instanceof ChangeStream);
            return done();
        });
    });

    describe('_handleChangeStreamErrorEvent', () =>  {
        it('should reset change stream on error without closing it (already closed)', async () => {
            const removeListenerStub = sinon.stub();
            const closeStub = sinon.stub();
            const startStub = sinon.stub(wrapper, 'start');
            const changehandler = sinon.stub().returns(true);
            wrapper._changeStream = {
                removeListener: removeListenerStub,
                isClosed: () => true,
                close: closeStub,
            };
            wrapper._changeHandler = changehandler;
            await wrapper._handleChangeStreamErrorEvent()
            .then(() => {
                assert(startStub.calledOnce);
                assert(closeStub.notCalled);
                assert(removeListenerStub.getCall(0).calledWithMatch('change',
                    changehandler));
                assert(removeListenerStub.getCall(1).calledWithMatch('error',
                    wrapper._handleChangeStreamErrorEvent.bind(wrapper)));
            })
            .catch(err => assert.ifError(err));
        });

        it('should close then reset the change steam on error', async () => {
            const removeListenerStub = sinon.stub();
            const closeStub = sinon.stub();
            const startStub = sinon.stub(wrapper, 'start');
            const changehandler = sinon.stub().returns(true);
            wrapper._changeStream = {
                removeListener: removeListenerStub,
                isClosed: () => false,
                close: closeStub,
            };
            wrapper._changeHandler = changehandler;
            await wrapper._handleChangeStreamErrorEvent()
            .then(() => {
                assert(startStub.calledOnce);
                assert(closeStub.calledOnce);
                assert(removeListenerStub.getCall(0).calledWithMatch('change',
                    changehandler));
                assert(removeListenerStub.getCall(1).calledWithMatch('error',
                    wrapper._handleChangeStreamErrorEvent.bind(wrapper)));
            })
            .catch(err => assert.ifError(err));
        });

        it('should throw error when throwOnError is set to true', async () => {
            const startStub = sinon.stub(wrapper, 'start');
            wrapper._changeStream = {
                removeListener: () => {},
                isClosed: () => true,
                close: () => {},
            };
            wrapper._throwOnError = true;
            await wrapper._handleChangeStreamErrorEvent()
            .then(() => assert(startStub.notCalled))
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });

    describe('start', () =>  {
        it('Should create and listen to the metastore change stream', done => {
            const watchStub = sinon.stub().returns(new events.EventEmitter());
            wrapper._collection = {
                watch: watchStub,
            };
            assert.doesNotThrow(() => {
                wrapper.start();
                assert(wrapper._changeStream instanceof events.EventEmitter);
                assert(watchStub.calledOnce);
                const changeHandlers = events.getEventListeners(wrapper._changeStream, 'change');
                const errorHandlers = events.getEventListeners(wrapper._changeStream, 'error');
                assert.strictEqual(changeHandlers.length, 1);
                assert.strictEqual(errorHandlers.length, 1);
                return done();
            });
        });

        it('Should set the change stream pipeline', done => {
            const watchStub = sinon.stub().returns(new events.EventEmitter());
            const changeStreamPipline = [
                {
                    $project: {
                        '_id': 1,
                        'operationType': 1,
                        'documentKey._id': 1,
                        'fullDocument.value': 1
                    },
                },
            ];
            const changeStreamParams = { fullDocument: 'updateLookup' };
            wrapper._collection = {
                watch: watchStub,
            };
            wrapper._pipeline = changeStreamPipline;
            assert.doesNotThrow(() => {
                wrapper.start();
                assert(watchStub.calledOnceWith(changeStreamPipline, changeStreamParams));
                return done();
            });
        });

        it('Should resume change stream using resumeToken', done => {
            const watchStub = sinon.stub().returns(new events.EventEmitter());
            wrapper._collection = {
                watch: watchStub,
            };
            wrapper._resumeToken = '1234';
            const changeStreamParams = {
                fullDocument: 'updateLookup',
                startAfter: '1234',
            };
            assert.doesNotThrow(() => {
                wrapper.start();
                assert(watchStub.calledOnceWith([], changeStreamParams));
                return done();
            });
        });

        it('Should fail if it fails to create change stream', done => {
            wrapper._collection = {
                watch: sinon.stub().throws(errors.InternalError),
            };
            assert.throws(() => wrapper.start());
            return done();
        });
    });
});
