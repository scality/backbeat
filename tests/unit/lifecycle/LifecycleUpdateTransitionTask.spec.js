const assert = require('assert');
const werelogs = require('werelogs');

const { ObjectMD } = require('arsenal').models;
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const LifecycleUpdateTransitionTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleUpdateTransitionTask');

class GarbageCollectorProducerMock {
    constructor() {
        this.receivedEntry = null;
    }

    publishActionEntry(gcEntry, done) {
        this.receivedEntry = gcEntry;
        return done();
    }

    getReceivedEntry() {
        return this.receivedEntry;
    }
}


class BackbeatClientMock {
    constructor() {
        this.mdObj = null;
        this.receivedMd = null;
    }

    setMdObj(mdObj) {
        this.mdObj = mdObj;
    }

    getMetadata(params, log, cb) {
        return cb(null, { Body: this.mdObj.getSerialized() });
    }

    putMetadata(params, log, cb) {
        this.receivedMd = JSON.parse(params.mdBlob);
        return cb();
    }

    getReceivedMd() {
        return this.receivedMd;
    }
}

class LifecycleObjectProcessorMock {
    constructor(backbeatClient, gcProducer) {
        this.backbeatClient = backbeatClient;
        this.gcProducer = gcProducer;
        this.logger = new werelogs.Logger('test:LifecycleUpdateTransitionTask');
    }

    getStateVars() {
        return {
            backbeatClient: this.backbeatClient,
            gcProducer: this.gcProducer,
            logger: this.logger,
        };
    }
}

describe('LifecycleUpdateTransitionTask', () => {
    let backbeatClient;
    let gcProducer;
    let objectProcessor;
    let mdObj;
    let actionEntry;
    let oldLocation;
    let newLocation;
    let task;
    beforeEach(() => {
        oldLocation = [{
            key: 'oldLocationKey',
            size: 10,
            start: 0,
            dataStoreName: 'oldLocationName',
            dataStoreType: 'aws_s3',
            dataStoreETag: 'oldETag',
            dataStoreVersionId: 'oldVersionId',
        }];
        newLocation = [{
            key: 'newLocationKey',
            size: 20,
            start: 0,
            dataStoreName: 'newLocationName',
            dataStoreType: 'gcp',
            dataStoreETag: 'newETag',
            dataStoreVersionId: 'newVersionId',
        }];

        backbeatClient = new BackbeatClientMock();
        gcProducer = new GarbageCollectorProducerMock();
        objectProcessor = new LifecycleObjectProcessorMock(
            backbeatClient, gcProducer);
        mdObj = new ObjectMD();
        mdObj.setLocation(oldLocation)
            .setContentMd5('1ccc7006b902a4d30ec26e9ddcf759d8')
            .setLastModified('1970-01-01T00:00:00.000Z');
        backbeatClient.setMdObj(mdObj);
        actionEntry = ActionQueueEntry.create('copyLocation')
            .setAttribute('target', {
                bucket: 'somebucket',
                key: 'somekey',
                eTag: '"1ccc7006b902a4d30ec26e9ddcf759d8"',
                lastModified: '1970-01-01T00:00:00.000Z'
            })
            .setSuccess({ location: newLocation });
        task = new LifecycleUpdateTransitionTask(objectProcessor);
    });

    it('should transition location in metadata', done => {
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            const receivedMd = backbeatClient.getReceivedMd();
            assert.deepStrictEqual(receivedMd.location, newLocation);
            const receivedGcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(receivedGcEntry.getActionType(), 'deleteData');
            assert.deepStrictEqual(
                receivedGcEntry.getAttribute('target.locations'), oldLocation);
            done();
        });
    });

    it('should not transition location in metadata and should GC new ' +
    'location if eTag does not match', done => {
        actionEntry.setAttribute('target.eTag',
                                 '"6713e7cf89b6b16d5abf11d1fabac587"');
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            const receivedMd = backbeatClient.getReceivedMd();
            assert.strictEqual(receivedMd, null);
            const receivedGcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(receivedGcEntry.getActionType(), 'deleteData');
            assert.deepStrictEqual(
                receivedGcEntry.getAttribute('target.locations'), newLocation);
            done();
        });
    });

    it('should not update metadata nor GC anything if location does not change',
    done => {
        mdObj.setLocation(newLocation);
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            const receivedMd = backbeatClient.getReceivedMd();
            assert.strictEqual(receivedMd, null);
            const receivedGcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(receivedGcEntry, null);
            done();
        });
    });

    it('should not transition location in metadata and should GC new ' +
    'location if LastModified does not match', done => {
        actionEntry.setAttribute('target.lastModified',
                                 '1970-01-01T00:00:00.001Z');
        task.processActionEntry(actionEntry, err => {
            assert.ifError(err);
            const receivedMd = backbeatClient.getReceivedMd();
            assert.strictEqual(receivedMd, null);
            const receivedGcEntry = gcProducer.getReceivedEntry();
            assert.strictEqual(receivedGcEntry.getActionType(), 'deleteData');
            assert.deepStrictEqual(
                receivedGcEntry.getAttribute('target.locations'), newLocation);
            done();
        });
    });
});
