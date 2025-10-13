'use strict';

const sinon = require('sinon');
const assert = require('assert');

const fakeLogger = require('../../../utils/fakeLogger');
const locationConfig = require('../../../../conf/locationConfig.json') || {};
const LocationStatusManager = require('../../../../lib/util/LocationStatusManager');

const zkNodes = {
    '/backbeat/crr/state/us-east-1': {
        paused: false,
    },
    '/backbeat/crr/state/us-east-2': {
        paused: false,
    },
};

const fakeMongoClient = {};
const fakeZkClient = {
    getData: (path, cb) => cb(null, Buffer.from(JSON.stringify(zkNodes[path]))),
};
const fakeRedisClient = {};

describe('LocationStatusManager', () => {
    let lsm;

    beforeEach(() => {
        lsm = new LocationStatusManager(
            fakeMongoClient,
            fakeZkClient,
            fakeRedisClient,
            {
                crr: {
                    namespace: '/backbeat/crr',
                    statePath: '/state',
                    topic: 'crr-topic',
                    isMongo: false,
                },
                ingestion: {
                    namespace: '/backbeat/ingestion',
                    statePath: '/state',
                    topic: 'ingestion-topic',
                    isMongo: false,
                },
                lifecycle: {
                    isMongo: true,
                },
            },
            fakeLogger,
        );
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('getServiceStatus', () => {
        it('should get service status for a location from ZooKeeper', done => {
            lsm.getServiceStatus('crr', ['us-east-1'], (err, statuses) => {
                assert.ifError(err);
                assert.deepStrictEqual(statuses, { 'us-east-1': 'enabled' });
                done();
            });
        });

        it('should get service status for a location from MongoDB', done => {
            const find = sinon.stub().returns({
                toArray: sinon.stub().resolves([
                    { _id: 'us-east-1', value: { lifecycle: { paused: true } } },
                ]),
            });
            lsm._locationStatusColl = { find };
            lsm.getServiceStatus('lifecycle', ['us-east-1'], (err, statuses) => {
                assert.ifError(err);
                assert.deepStrictEqual(statuses, { 'us-east-1': 'disabled' });
                done();
            });
        });
    });

    describe('getResumeSchedule', () => {
        it('should get resume schedule for a location from ZooKeeper', done => {
            const getData = sinon.stub().callsFake((path, cb) => {
                const data = { paused: true, scheduledResume: '2023-01-01T00:00:00.000Z' };
                cb(null, Buffer.from(JSON.stringify(data)));
            });
            lsm._zkClient.getData = getData;
            lsm.getResumeSchedule('crr', ['us-east-1'], (err, schedules) => {
                assert.ifError(err);
                assert.deepStrictEqual(schedules, { 'us-east-1': '2023-01-01T00:00:00.000Z' });
                done();
            });
        });

        it('should get resume schedule for a location from MongoDB', done => {
            const find = sinon.stub().returns({
                toArray: sinon.stub().resolves([
                    {
                        _id: 'us-east-1',
                        value: { lifecycle: { paused: true, scheduledResume: '2023-01-01T00:00:00.000Z' } }
                    },
                ]),
            });
            lsm._locationStatusColl = { find };
            lsm.getResumeSchedule('lifecycle', ['us-east-1'], (err, schedules) => {
                assert.ifError(err);
                assert.deepStrictEqual(schedules, { 'us-east-1': '2023-01-01T00:00:00.000Z' });
                done();
            });
        });
    });

    describe('pauseService', () => {
        it('should publish pause action to Redis for non-mongo service', done => {
            const publish = sinon.spy();
            lsm._redis = { publish };
            lsm.pauseService('crr', ['us-east-1', 'us-west-2'], err => {
                assert.ifError(err);
                assert(publish.calledWith('crr-topic-us-east-1', JSON.stringify({ action: 'pauseService' })));
                assert(publish.calledWith('crr-topic-us-west-2', JSON.stringify({ action: 'pauseService' })));
                done();
            });
        });

        it('should update service status in MongoDB for mongo service', done => {
            const updateOne = sinon.stub().resolves();
            lsm._locationStatusColl = { updateOne };
            lsm.pauseService('lifecycle', ['us-east-1', 'us-west-2'], err => {
                assert.ifError(err);
                assert(updateOne.calledTwice);
                done();
            });
        });
    });

    describe('resumeService', () => {
        it('should publish resume action to Redis for non-mongo service', done => {
            const publish = sinon.spy();
            lsm._redis = { publish };
            lsm.resumeService('crr', ['us-east-1'], null, null, err => {
                assert.ifError(err);
                assert(publish.calledOnceWith('crr-topic-us-east-1', JSON.stringify({ action: 'resumeService' })));
                done();
            });
        });

        it('should update service status in MongoDB for mongo service', done => {
            const updateOne = sinon.stub().resolves();
            lsm._locationStatusColl = { updateOne };
            lsm.resumeService('lifecycle', ['us-east-1'], null, null, err => {
                assert.ifError(err);
                assert(updateOne.calledOnce);
                done();
            });
        });

        it('should schedule resume for a mongo service', done => {
            const clock = sinon.useFakeTimers();
            const updateOne = sinon.stub().resolves({ modifiedCount: 1 });
            lsm._locationStatusColl = { updateOne };

            lsm.resumeService('lifecycle', ['us-east-1'], true, '{ "hours": 1 }', err => {
                assert.ifError(err);
                assert(updateOne.calledOnce);

                clock.tick(3600000);

                assert(updateOne.calledTwice);
                const secondCallArgs = updateOne.secondCall.args;
                assert.deepStrictEqual(secondCallArgs[1], {
                    $set: {
                        'value.lifecycle': {
                            paused: false,
                            scheduledResume: null,
                        },
                    },
                });

                clock.restore();
                done();
            });
        });
    });

    describe('deleteScheduledResumeService', () => {
        it('should publish delete schedule action to Redis for non-mongo service', done => {
            const publish = sinon.spy();
            lsm._redis = { publish };
            lsm.deleteScheduledResumeService('crr', ['us-east-1'], err => {
                assert.ifError(err);
                assert(publish.calledOnceWith('crr-topic-us-east-1',
                    JSON.stringify({ action: 'deleteScheduledResumeService' })));
                done();
            });
        });

        it('should update service status in MongoDB for mongo service', done => {
            const updateOne = sinon.stub().resolves();
            lsm._locationStatusColl = { updateOne };
            lsm.deleteScheduledResumeService('lifecycle', ['us-east-1'], err => {
                assert.ifError(err);
                assert(updateOne.calledOnce);
                done();
            });
        });
    });

    describe('_setupLocationStatusStore', () => {
        let cluster;
        let createCollection;
        let collection;

        beforeEach(() => {
            cluster = require('cluster');
            createCollection = sinon.stub().resolves();
            collection = sinon.stub().returns({});
            lsm._mongoClient = { createCollection, collection };
        });

        it('should not run setup if process is a worker', done => {
            sinon.stub(cluster, 'isWorker').value(true);
            const waterfall = sinon.spy(lsm, '_listCollectionDocuments');

            lsm._setupLocationStatusStore(err => {
                assert.ifError(err);
                assert(createCollection.calledOnce);
                assert(waterfall.notCalled);
                done();
            });
        });

        it('should run setup if process is primary', done => {
            sinon.stub(cluster, 'isWorker').value(false);
            const listCollectionDocuments = sinon.stub(lsm, '_listCollectionDocuments').yields(null, []);
            const deleteInvalidLocations = sinon.stub(lsm, '_deleteInvalidLocations').yields(null, {});
            const handleScheduledResume = sinon.stub(lsm, '_handleScheduledResume').yields(null, {});
            const addNewLocations = sinon.stub(lsm, '_addNewLocations').yields(null);

            lsm._setupLocationStatusStore(err => {
                assert.ifError(err);
                assert(createCollection.calledOnce);
                assert(listCollectionDocuments.calledOnce);
                assert(deleteInvalidLocations.calledOnce);
                assert(handleScheduledResume.calledOnce);
                assert(addNewLocations.calledOnce);
                done();
            });
        });

        it('should return error if creating collection fails', done => {
            const err = new Error('mongo error');
            createCollection.rejects(err);
            sinon.stub(cluster, 'isWorker').value(false);
            const waterfall = sinon.spy(lsm, '_listCollectionDocuments');

            lsm._setupLocationStatusStore(setupErr => {
                assert.deepStrictEqual(setupErr, err);
                assert(createCollection.calledOnce);
                assert(waterfall.notCalled);
                done();
            });
        });
    });

    describe('_parseScheduleResumeBody', () => {
        it('should return default hours if body is empty', () => {
            const { error, hours } = lsm._parseScheduleResumeBody(null);
            assert.ifError(error);
            assert.strictEqual(hours, 6);
        });

        it('should return default hours if hours are not in body', () => {
            const { error, hours } = lsm._parseScheduleResumeBody('{}');
            assert.ifError(error);
            assert.strictEqual(hours, 6);
        });

        it('should return error for invalid hours', () => {
            const { error } = lsm._parseScheduleResumeBody('{ "hours": "abc" }');
            assert(error);
            assert(error.description.includes('hours must be an integer greater than 0'));
        });

        it('should return error for non-positive hours', () => {
            const { error } = lsm._parseScheduleResumeBody('{ "hours": 0 }');
            assert(error);
            assert(error.description.includes('hours must be an integer greater than 0'));
        });

        it('should return parsed hours', () => {
            const { error, hours } = lsm._parseScheduleResumeBody('{ "hours": 12 }');
            assert.ifError(error);
            assert.strictEqual(hours, 12);
        });

        it('should return error for malformed JSON', () => {
            const { error } = lsm._parseScheduleResumeBody('{ "hours": 12');
            assert(error);
            assert(error.description.includes('The body of your POST request is not well-formed'));
        });
    });

    describe('_initCollection', () => {
        it('should create collection if it does not exist', done => {
            const createCollection = sinon.stub().resolves();
            const collection = sinon.stub().returns({});
            lsm._mongoClient = { createCollection, collection };

            lsm._initCollection(err => {
                assert.ifError(err);
                assert(createCollection.calledOnce);
                assert(collection.calledOnce);
                done();
            });
        });

        it('should not create collection if it already exists', done => {
            const err = new Error('NamespaceExists');
            err.codeName = 'NamespaceExists';
            const createCollection = sinon.stub().rejects(err);
            const collection = sinon.stub().returns({});
            lsm._mongoClient = { createCollection, collection };

            lsm._initCollection(err => {
                assert.ifError(err);
                assert(createCollection.calledOnce);
                assert(collection.calledOnce);
                done();
            });
        });

        it('should return error if mongo fails', done => {
            const err = new Error('mongo error');
            const createCollection = sinon.stub().rejects(err);
            lsm._mongoClient = { createCollection };

            lsm._initCollection(initErr => {
                assert.deepStrictEqual(initErr, err);
                done();
            });
        });
    });

    describe('_listCollectionDocuments', () => {
        it('should list documents from collection', done => {
            const toArray = sinon.stub().resolves([]);
            const find = sinon.stub().returns({ toArray });
            lsm._locationStatusColl = { find };

            lsm._listCollectionDocuments((err, docs) => {
                assert.ifError(err);
                assert.deepStrictEqual(docs, []);
                assert(find.calledOnce);
                assert(toArray.calledOnce);
                done();
            });
        });

        it('should return error if listing fails', done => {
            const err = new Error('mongo error');
            const toArray = sinon.stub().rejects(err);
            const find = sinon.stub().returns({ toArray });
            lsm._locationStatusColl = { find };

            lsm._listCollectionDocuments(listErr => {
                assert.deepStrictEqual(listErr, err);
                done();
            });
        });
    });

    describe('_handleScheduledResume', () => {
        let schedule;
        beforeEach(() => {
            schedule = require('node-schedule');
            sinon.stub(schedule, 'scheduleJob');
        });

        it('should schedule resume jobs for locations', done => {
            const currentDate = new Date();
            const locations = {
                'us-east-1': { lifecycle: { scheduledResume: currentDate.setHours(currentDate.getHours() + 1) } },
            };
            lsm._handleScheduledResume(locations, err => {
                assert.ifError(err);
                assert(schedule.scheduleJob.calledOnce);
                done();
            });
        });
    });

    describe('_deleteInvalidLocations', () => {
        it('should delete invalid locations from mongo', done => {
            const deleteMany = sinon.stub().resolves();
            lsm._locationStatusColl = { deleteMany };
            const locations = [
                { _id: 'us-east-1', value: {} },
                { _id: 'invalid-location', value: {} },
            ];
            lsm._deleteInvalidLocations(locations, (err, validLocations) => {
                assert.ifError(err);
                assert(validLocations['us-east-1']);
                assert(deleteMany.calledOnceWith({ _id: { $in: ['invalid-location'] } }));
                done();
            });
        });
    });

    describe('_addNewLocations', () => {
        it('should add new locations to mongo', done => {
            const insertOne = sinon.stub().resolves();
            lsm._locationStatusColl = { insertOne };
            const locations = [{ _id: 'us-east-1', value: {} }];
            const validLocationNames = Object.keys(locationConfig);
            const newLocation = validLocationNames.find(loc => loc !== 'us-east-1');

            lsm._addNewLocations(locations, err => {
                assert.ifError(err);
                assert(insertOne.calledWith({
                    _id: newLocation,
                    value: {
                        crr: null,
                        ingestion: null,
                        lifecycle: { paused: false, scheduledResume: null },
                    },
                }));
                done();
            });
        });
    });

    describe('_updateServiceStatusForLocation', () => {
        it('should update service status for a location', done => {
            const updateOne = sinon.stub().resolves({ modifiedCount: 1 });
            lsm._locationStatusColl = { updateOne };
            const query = { _id: 'us-east-1' };
            const update = { $set: { 'value.lifecycle.paused': true } };
            lsm._updateServiceStatusForLocation(query, update, err => {
                assert.ifError(err);
                assert(updateOne.calledOnceWith(query, update, { upsert: false }));
                done();
            });
        });

        it('should return error if update fails', done => {
            const err = new Error('mongo error');
            const updateOne = sinon.stub().rejects(err);
            lsm._locationStatusColl = { updateOne };
            const query = { _id: 'us-east-1' };
            const update = { $set: { 'value.lifecycle.paused': true } };
            lsm._updateServiceStatusForLocation(query, update, updateErr => {
                assert.deepStrictEqual(updateErr, err);
                done();
            });
        });
    });

    describe('_getMongoStateDetails', () => {
        it('should get state details from mongo', done => {
            const find = sinon.stub().returns({
                toArray: sinon.stub().resolves([
                    { _id: 'us-east-1', value: { lifecycle: { paused: true } } },
                ]),
            });
            lsm._locationStatusColl = { find };
            lsm._getMongoStateDetails('lifecycle', ['us-east-1'], (err, states) => {
                assert.ifError(err);
                assert.deepStrictEqual(states, { 'us-east-1': { paused: true } });
                done();
            });
        });

        it('should return error if mongo fails', done => {
            const err = new Error('mongo error');
            const find = sinon.stub().returns({
                toArray: sinon.stub().rejects(err),
            });
            lsm._locationStatusColl = { find };
            lsm._getMongoStateDetails('lifecycle', ['us-east-1'], getErr => {
                assert.deepStrictEqual(getErr, err);
                done();
            });
        });
    });

    describe('_getZkStateDetails', () => {
        it('should return error if zookeeper fails', done => {
            const err = new Error('zk error');
            const getData = sinon.stub().callsFake((path, cb) => cb(err));
            lsm._zkClient.getData = getData;
            lsm._getZkStateDetails('crr', ['us-east-1'], getErr => {
                assert(getErr);
                done();
            });
        });

        it('should return error if zookeeper node does not exist', done => {
            const err = new Error('no node');
            err.name = 'NO_NODE';
            const getData = sinon.stub().callsFake((path, cb) => cb(err));
            lsm._zkClient.getData = getData;
            lsm._getZkStateDetails('crr', ['us-east-1'], getErr => {
                assert(getErr);
                done();
            });
        });

        it('should return error if data is malformed', done => {
            const getData = sinon.stub().callsFake((path, cb) => cb(null, Buffer.from('not json')));
            lsm._zkClient.getData = getData;
            lsm._getZkStateDetails('crr', ['us-east-1'], getErr => {
                assert(getErr);
                done();
            });
        });
    });
});
