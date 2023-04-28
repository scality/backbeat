'use strict'; // eslint-disable-line

const assert = require('assert');
const sinon = require('sinon');
const events = require('events');
const { MongoClient } = require('mongodb');

const LocationStatusStream = require('../../../extensions/utils/LocationStatusStream');
const ChangeStream = require('../../../lib/wrappers/ChangeStream');
const FakeLogger = require('../../utils/fakeLogger');

const {
    mongoConfig,
} = require('../../functional/lifecycle/configObjects');


describe('LocationStatusStream', () => {

    afterEach(() => {
        sinon.restore();
    });

    describe('_setupMongoClient', () => {
        it('should connect to mongo and setup client', async () => {
            const collectionStub = sinon.stub();
            const mongoCommandStub = sinon.stub().returns({
                version: '4.3.17',
            });
            const dbStub = sinon.stub().returns({
                collection: collectionStub,
                command: mongoCommandStub,
            });
            const mongoConnectStub = sinon.stub(MongoClient, 'connect').resolves({
                db: dbStub,
            });
            const lss = new LocationStatusStream('lifecycle', mongoConfig, null, null, FakeLogger);
            lss._setupMongoClient(err => {
                assert.ifError(err);
                const mongoUrl = 'mongodb://user:password@localhost:27017,localhost:27018,' +
                'localhost:27019/?w=majority&readPreference=primary&replicaSet=rs0';
                assert(mongoConnectStub.calledOnceWith(
                    mongoUrl,
                    {
                        replicaSet: 'rs0',
                        useNewUrlParser: true,
                        useUnifiedTopology: true,
                    }
                ));
                assert(dbStub.calledOnceWith('metadata', { ignoreUndefined: true }));
                assert(collectionStub.calledOnceWith('__locationStatusStore'));
                assert(mongoCommandStub.calledOnceWith({
                    buildInfo: 1,
                }));
                assert.equal(lss._mongoVersion, '4.3.17');
            });
        });
    });

    it('_initializeLocationStatuses:: should initialize paused locations', done => {
        const locations = [
            {
                _id: 'us-east-1',
                value: {
                    lifecycle: {
                        paused: true,
                    },
                },
            },
            {
                _id: 'us-east-2',
                value: {
                    lifecycle: {
                        paused: true,
                    },
                },
            },
            {
                _id: 'us-east-3',
                value: {
                    lifecycle: {
                        paused: true,
                    },
                },
            },
        ];
        const pauseStub = sinon.stub();
        const lss = new LocationStatusStream('lifecycle', mongoConfig, pauseStub, null, FakeLogger);
        lss._locationStatusColl = {
            find: () => ({
                toArray: sinon.stub().yields(null, locations),
            }),
        };
        lss._initializeLocationStatuses(err => {
            assert.ifError(err);
            assert(pauseStub.getCall(0).calledWith('us-east-1'));
            assert(pauseStub.getCall(1).calledWith('us-east-2'));
            assert(pauseStub.getCall(2).calledWith('us-east-3'));
            return done();
        });
    });

    [
        {
            case: 'should correctly pause location',
            event: {
                operationType: 'update',
                documentKey: {
                    _id: 'us-east-1',

                },
                fullDocument: {
                    _id: 'us-east-1',
                    value: {
                        lifecycle: {
                            paused: true,
                        },
                    },
                },
            },
            expectedFunctionCall: 'pause',
        },
        {
            case: 'should correctly resume location',
            event: {
                operationType: 'update',
                documentKey: {
                    _id: 'us-east-1',

                },
                fullDocument: {
                    _id: 'us-east-1',
                    value: {
                        lifecycle: {
                            paused: false,
                        },
                    },
                },
            },
            expectedFunctionCall: 'resume',
        },
        {
            case: 'should remove deleted location',
            event: {
                operationType: 'delete',
                documentKey: {
                    _id: 'us-east-1',

                },
                fullDocument: null,
            },
            expectedFunctionCall: 'resume',
        }
    ].forEach(params => {
        it(`_handleChangeStreamChangeEvent:: ${params.case}`, () => {
            const pauseStub = sinon.stub();
            const resumeStub = sinon.stub();
            const lss = new LocationStatusStream('lifecycle', mongoConfig, pauseStub, resumeStub, FakeLogger);
            lss._handleChangeStreamChangeEvent(params.event);
            const fn = params.expectedFunctionCall === 'pause' ? pauseStub : resumeStub;
            assert(fn.calledOnceWith(params.event.documentKey._id));
        });
    });

    it('should use resumeAfter with mongo 3.6', done => {
        const lss = new LocationStatusStream('lifecycle', mongoConfig, null, null, FakeLogger);
        lss._mongoVersion = '3.6.2';
        lss._locationStatusColl = {
            watch: sinon.stub().returns(new events.EventEmitter()),
        };
        lss._setChangeStream(() => {
            assert(lss._changeStreamWrapper instanceof ChangeStream);
            assert.equal(lss._changeStreamWrapper._resumeField, 'resumeAfter');
            done();
        });
    });

    it('should use resumeAfter with mongo 4.0', done => {
        const lss = new LocationStatusStream('lifecycle', mongoConfig, null, null, FakeLogger);
        lss._mongoVersion = '4.0.7';
        lss._locationStatusColl = {
            watch: sinon.stub().returns(new events.EventEmitter()),
        };
        lss._setChangeStream(() => {
            assert(lss._changeStreamWrapper instanceof ChangeStream);
            assert.equal(lss._changeStreamWrapper._resumeField, 'resumeAfter');
            done();
        });
    });

    it('should use startAfter with mongo 4.2', done => {
        const lss = new LocationStatusStream('lifecycle', mongoConfig, null, null, FakeLogger);
        lss._mongoVersion = '4.2.3';
        lss._locationStatusColl = {
            watch: sinon.stub().returns(new events.EventEmitter()),
        };
        lss._setChangeStream(() => {
            assert(lss._changeStreamWrapper instanceof ChangeStream);
            assert.equal(lss._changeStreamWrapper._resumeField, 'startAfter');
            done();
        });
    });
});
