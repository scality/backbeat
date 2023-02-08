'use strict'; // eslint-disable-line

const assert = require('assert');
const sinon = require('sinon');

const LocationStatusStream = require('../../../extensions/utils/LocationStatusStream');
const FakeLogger = require('../../utils/fakeLogger');

const {
    mongoConfig,
} = require('../../functional/lifecycle/configObjects');


describe('LocationStatusStream', () => {

    afterEach(() => {
        sinon.restore();
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
});
