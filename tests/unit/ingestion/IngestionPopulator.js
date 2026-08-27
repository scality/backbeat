'use strict';

const assert = require('assert');
const sinon = require('sinon');
const errors = require('arsenal').errors;
const config = require('../../../lib/Config');
const IngestionPopulator =
    require('../../../lib/queuePopulator/IngestionPopulator');
const IngestionReader = require('../../../lib/queuePopulator/IngestionReader');
const BackbeatProducer = require('../../../lib/BackbeatProducer');
const fakeLogger = require('../../utils/fakeLogger');

const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const qpConfig = config.queuePopulator;
const mConfig = config.metrics;
const rConfig = config.redis;
const ingestionConfig = config.extensions.ingestion;
const s3Config = config.s3;

// zenko bucket naming to be used to test with
const EXISTING_BUCKET = 'my-zenko-bucket';
const NEW_BUCKET = 'your-zenko-bucket';
const OLD_BUCKET = 'old-ingestion-bucket';

const oldLocation = {
    'old-ring': {
        details: {
            credentials: {
                accessKey: 'myAccessKey',
                secretKey: 'myVerySecretKey',
            },
            awsEndpoint: '127.0.0.1:80',
            https: false,
            bucketName: 'old-ring-bucket',
        },
        // maybe remove
        locationType: 'location-scality-ring-s3-v1',
        type: 'aws_s3',
    },
};
const existingLocation = {
    'existing-ring': {
        details: {
            credentials: {
                accessKey: 'myAccessKey',
                secretKey: 'myVerySecretKey',
            },
            awsEndpoint: '127.0.0.1:8000',
            bucketName: 'existing-ring-bucket',
            https: false,
        },
        type: 'aws_s3',
        // maybe remove
        locationType: 'location-scality-ring-s3-v1',
    },
};
const newLocation = {
    'new-ring': {
        details: {
            credentials: {
                accessKey: 'yourAccessKey',
                secretKey: 'yourVerySecretKey',
            },
            awsEndpoint: '127.0.0.1',
            bucketName: 'new-ring-bucket',
            https: false,
        },
        type: 'aws_s3',
        // maybe remove
        locationType: 'location-scality-ring-s3-v1',
    },
};

const oldBucket = {
    locationConstraint: 'old-ring',
    name: OLD_BUCKET,
    ingestion: { status: 'enabled' },
};
const existingBucket = {
    locationConstraint: 'existing-ring',
    name: EXISTING_BUCKET,
    ingestion: { status: 'enabled' },
};
const newBucket = {
    locationConstraint: 'new-ring',
    name: NEW_BUCKET,
    ingestion: { status: 'enabled' },
};

// To be mocked as existing or currently active
const previousLocations = Object.assign({}, oldLocation, existingLocation);
const previousBuckets = [oldBucket, existingBucket];

// To be mocked as incoming new active
const currentLocations = Object.assign({}, existingLocation, newLocation);
const currentBuckets = [existingBucket, newBucket];

class IngestionReaderMock extends IngestionReader {
    reset() {
        this._updated = false;
    }

    hasUpdated() {
        return this._updated;
    }

    /**
     * Mock to avoid creating S3 client, avoid decrypting secret key.
     * `IngestionReader.refresh` is called to check and update IngestionReaders.
     * Every time this method is called indicates a valid update was found.
     * @return {undefined}
     */
    _setupIngestionProducer() {
        this._updated = true;
    }
}

class IngestionPopulatorMock extends IngestionPopulator {
    reset() {
        this._added = [];
        this._removed = [];
        this._ingestionSources = {};
    }

    getAdded() {
        return this._added;
    }

    getRemoved() {
        return this._removed;
    }

    getUpdated() {
        const updated = [];
        Object.keys(this._ingestionSources).forEach(s => {
            if (this._ingestionSources[s].hasUpdated()) {
                updated.push(s);
            }
        });
        return updated;
    }

    _setupPriorState() {
        config.setIngestionBuckets(previousLocations, previousBuckets);
        this.applyUpdates();
        this._added = [];
        this._removed = [];
    }

    setupMock() {
        // for testing purposes
        this.reset();

        this._setupPriorState();
        config.setIngestionBuckets(currentLocations, currentBuckets);
    }

    _setupZkLocationNode(list, cb) {
        // overwrite and ignore creation of zookeeper nodes
        return cb();
    }

    addNewLogSource(newSource) {
        const zenkoBucket = newSource.name;
        this._ingestionSources[zenkoBucket] = new IngestionReaderMock({
            bucketdConfig: newSource,
            logger: fakeLogger,
            ingestionConfig: {},
        });
        this._added.push(newSource);
    }

    _closeLogState(source) {
        this._removed.push(source);
    }
}

describe('Ingestion Populator', () => {
    let ip;

    beforeEach(() => {
        ip = new IngestionPopulatorMock(
            zkConfig,
            kafkaConfig,
            qpConfig,
            mConfig,
            rConfig,
            ingestionConfig,
            s3Config,
        );
        ip.setupMock();
        ip.applyUpdates();
    });

    it('should fetch correctly formed ingestion bucket object information', () => {
        const buckets = config.getIngestionBuckets();
        buckets.forEach(bucket => {
            assert(bucket.credentials.accessKey);
            assert(bucket.credentials.secretKey);
            assert(bucket.awsEndpoint);
            assert(bucket.locationType);
            assert.strictEqual(bucket.locationType, 'scality_s3');
            assert.strictEqual(typeof bucket.https, 'boolean');
            assert(bucket.bucketName);
            assert(bucket.zenkoBucket);
            assert(bucket.ingestion);
            assert(bucket.locationConstraint);
        });
    });

    describe('applyUpdates helper method', () => {
        it('should attach configuration properties for each new ingestion ' +
        'source', () => {
            ip.getAdded().forEach(newSource => {
                assert(newSource.name);
                assert(newSource.bucket);
                assert(newSource.host);
                assert.strictEqual(typeof newSource.port, 'number');
                assert.strictEqual(typeof newSource.https, 'boolean');
                assert(newSource.type);
                assert(newSource.auth);
                assert(newSource.auth.accessKey);
                assert(newSource.auth.secretKey);
            });
        });

        it('should apply default port 80 for a new ingestion source with ' +
        'no port provided', () => {
            const source = ip.getAdded().find(newSource =>
                newSource.name === NEW_BUCKET
            );
            assert.strictEqual(source.port, 80);
        });

        it('should keep an existing active ingestion source', () => {
            const addedIndex = ip.getAdded().findIndex(r => r.name === EXISTING_BUCKET);
            const wasRemoved = ip.getRemoved().includes(EXISTING_BUCKET);

            assert.strictEqual(addedIndex, -1);
            assert(!wasRemoved);
        });

        it('should add a new ingestion source', () => {
            const addedIndex = ip.getAdded().findIndex(r => r.name === NEW_BUCKET);
            const wasRemoved = ip.getRemoved().includes(NEW_BUCKET);

            assert.notStrictEqual(addedIndex, -1);
            assert(!wasRemoved);
        });

        it('should remove an ingestion source that is has become inactive',
        () => {
            const addedIndex = ip.getAdded().findIndex(r => r.name === OLD_BUCKET);
            const wasRemoved = ip.getRemoved().includes(OLD_BUCKET);

            assert.strictEqual(addedIndex, -1);
            assert(wasRemoved);
        });

        it('should update an ingestion reader when the ingestion source ' +
        'information is updated', () => {
            assert.deepStrictEqual(ip.getUpdated(), []);

            // hack to update a valid editable field
            const locationName = Object.keys(existingLocation)[0];
            // full deep copy using JSON
            const dupeExistingLoc = JSON.parse(JSON.stringify(existingLocation));
            dupeExistingLoc[locationName].details.credentials.accessKey = 'anUpdatedKey';

            config.setIngestionBuckets(dupeExistingLoc, [existingBucket]);

            ip.applyUpdates();
            const updated = ip.getUpdated();

            assert.strictEqual(updated.length, 1);
            assert.strictEqual(updated[0], EXISTING_BUCKET);
        });
    });

    describe('_setupUpdatedReaders', () => {
        const FAILING_BUCKET = 'failing-zenko-bucket';
        const WORKING_BUCKET = 'working-zenko-bucket';

        /**
         * @param {string} zenkoBucket - target zenko bucket of the reader
         * @param {Error|null} setupError - error to fail `setup` with
         * @return {object} the stubbed reader
         */
        function createLogReaderMock(zenkoBucket, setupError) {
            const logReader = sinon.createStubInstance(IngestionReader);
            logReader.getTargetZenkoBucketName.returns(zenkoBucket);
            logReader.setup.yieldsAsync(setupError);
            return logReader;
        }

        beforeEach(() => {
            ip.logReaders = [];
            ip.logReadersUpdate = [];
        });

        it('should activate a log reader once its setup succeeds', done => {
            const logReaderMock = createLogReaderMock(WORKING_BUCKET, null);
            ip._ingestionSources[WORKING_BUCKET] = logReaderMock;
            ip.logReadersUpdate = [logReaderMock];

            ip._setupUpdatedReaders(err => {
                assert.ifError(err);
                assert.deepStrictEqual(ip.logReaders, [logReaderMock]);
                assert.deepStrictEqual(ip.logReadersUpdate, []);
                done();
            });
        });

        it('should queue a log reader again when its setup fails', done => {
            const logReaderMock =
                createLogReaderMock(FAILING_BUCKET, errors.InternalError);
            ip._ingestionSources[FAILING_BUCKET] = logReaderMock;
            ip.logReadersUpdate = [logReaderMock];

            ip._setupUpdatedReaders(err => {
                assert.ifError(err);
                assert.deepStrictEqual(ip.logReaders, []);
                assert.deepStrictEqual(ip.logReadersUpdate, [logReaderMock]);
                done();
            });
        });

        it('should not queue a log reader again when its setup fails and ' +
        'its source is no longer configured', done => {
            const logReaderMock =
                createLogReaderMock(FAILING_BUCKET, errors.InternalError);
            delete ip._ingestionSources[FAILING_BUCKET];
            ip.logReadersUpdate = [logReaderMock];

            ip._setupUpdatedReaders(err => {
                assert.ifError(err);
                assert.deepStrictEqual(ip.logReaders, []);
                assert.deepStrictEqual(ip.logReadersUpdate, []);
                done();
            });
        });

        it('should not queue a log reader again when its setup fails and ' +
        'its source has been registered with another reader', done => {
            const staleReader =
                createLogReaderMock(FAILING_BUCKET, errors.InternalError);
            const currentReader = createLogReaderMock(FAILING_BUCKET, null);
            ip._ingestionSources[FAILING_BUCKET] = currentReader;
            ip.logReadersUpdate = [staleReader];

            ip._setupUpdatedReaders(err => {
                assert.ifError(err);
                assert.deepStrictEqual(ip.logReaders, []);
                assert.deepStrictEqual(ip.logReadersUpdate, []);
                done();
            });
        });

        it('should not activate a log reader when its setup succeeds and ' +
        'its source is no longer configured', done => {
            const logReaderMock = createLogReaderMock(WORKING_BUCKET, null);
            delete ip._ingestionSources[WORKING_BUCKET];
            ip.logReadersUpdate = [logReaderMock];

            ip._setupUpdatedReaders(err => {
                assert.ifError(err);
                assert.deepStrictEqual(ip.logReaders, []);
                assert.deepStrictEqual(ip.logReadersUpdate, []);
                done();
            });
        });

        it('should not activate a log reader when its setup succeeds and ' +
        'its source has been registered with another reader', done => {
            const staleReader = createLogReaderMock(WORKING_BUCKET, null);
            const currentReader = createLogReaderMock(WORKING_BUCKET, null);
            ip._ingestionSources[WORKING_BUCKET] = currentReader;
            ip.logReadersUpdate = [staleReader];

            ip._setupUpdatedReaders(err => {
                assert.ifError(err);
                assert.deepStrictEqual(ip.logReaders, []);
                assert.deepStrictEqual(ip.logReadersUpdate, []);
                done();
            });
        });

        it('should keep setting up other log readers when one fails', done => {
            const failingReader =
                createLogReaderMock(FAILING_BUCKET, errors.InternalError);
            const workingReader = createLogReaderMock(WORKING_BUCKET, null);
            ip._ingestionSources[FAILING_BUCKET] = failingReader;
            ip._ingestionSources[WORKING_BUCKET] = workingReader;
            ip.logReadersUpdate = [failingReader, workingReader];

            ip._setupUpdatedReaders(err => {
                assert.ifError(err);
                assert.deepStrictEqual(ip.logReaders, [workingReader]);
                assert.deepStrictEqual(ip.logReadersUpdate, [failingReader]);
                done();
            });
        });
    });

    describe('removing a source while its setup is in flight', () => {
        const REMOVED_BUCKET = 'removed-zenko-bucket';

        // `IngestionPopulatorMock` stubs out `_closeLogState`, so a real
        // populator is needed to exercise the removal path.
        let populator;

        beforeEach(() => {
            populator = new IngestionPopulator(
                zkConfig,
                kafkaConfig,
                qpConfig,
                mConfig,
                rConfig,
                ingestionConfig,
                s3Config,
            );
        });

        /**
         * Build a reader whose `setup` stays pending until it is released, to
         * hold the reader in neither `logReaders` nor `logReadersUpdate`.
         *
         * @param {string} zenkoBucket - target zenko bucket of the reader
         * @param {Error|null} setupError - error to fail `setup` with
         * @return {object} the stubbed reader and the release function
         */
        function createPendingLogReaderMock(zenkoBucket, setupError) {
            const logReader = sinon.createStubInstance(IngestionReader);
            logReader.getTargetZenkoBucketName.returns(zenkoBucket);

            let setupCb = null;
            logReader.setup.callsFake(cb => {
                setupCb = cb;
            });

            return { logReader, release: () => setupCb(setupError) };
        }

        it('should not queue a log reader again when its source is removed ' +
        'while its setup fails', done => {
            const { logReader, release } =
                createPendingLogReaderMock(REMOVED_BUCKET, errors.InternalError);
            populator._ingestionSources[REMOVED_BUCKET] = logReader;
            populator.logReadersUpdate = [logReader];

            populator._setupUpdatedReaders(err => {
                assert.ifError(err);
                assert.deepStrictEqual(populator.logReadersUpdate, []);
                done();
            });

            populator._closeLogState(REMOVED_BUCKET);
            assert.strictEqual(populator._ingestionSources[REMOVED_BUCKET],
                undefined);

            release();
        });

        it('should not activate a log reader when its source is removed ' +
        'while its setup succeeds', done => {
            const { logReader, release } =
                createPendingLogReaderMock(REMOVED_BUCKET, null);
            populator._ingestionSources[REMOVED_BUCKET] = logReader;
            populator.logReadersUpdate = [logReader];

            populator._setupUpdatedReaders(err => {
                assert.ifError(err);
                assert.deepStrictEqual(populator.logReaders, []);
                assert.deepStrictEqual(populator.logReadersUpdate, []);
                done();
            });

            populator._closeLogState(REMOVED_BUCKET);
            release();
        });

        it('should not activate a log reader when its source is replaced ' +
        'while its setup succeeds', done => {
            const { logReader, release } =
                createPendingLogReaderMock(REMOVED_BUCKET, null);
            const freshReader = sinon.createStubInstance(IngestionReader);
            freshReader.getTargetZenkoBucketName.returns(REMOVED_BUCKET);

            populator._ingestionSources[REMOVED_BUCKET] = logReader;
            populator.logReadersUpdate = [logReader];

            populator._setupUpdatedReaders(err => {
                assert.ifError(err);
                assert.deepStrictEqual(populator.logReaders, []);
                assert.deepStrictEqual(populator.logReadersUpdate, []);
                // the reader registered in the meantime is left untouched
                assert.strictEqual(populator._ingestionSources[REMOVED_BUCKET],
                    freshReader);
                done();
            });

            // the source is removed, then configured again before the setup
            // of the first reader completes
            populator._closeLogState(REMOVED_BUCKET);
            populator._ingestionSources[REMOVED_BUCKET] = freshReader;

            release();
        });
    });

    describe('_closeLogState', () => {
        const ACTIVE_BUCKET = 'active-zenko-bucket';
        const PENDING_BUCKET = 'pending-zenko-bucket';

        // `IngestionPopulatorMock` stubs out `_closeLogState`, so a real
        // populator is needed to exercise it.
        let populator;
        let removeReaderState;

        beforeEach(() => {
            populator = new IngestionPopulator(
                zkConfig,
                kafkaConfig,
                qpConfig,
                mConfig,
                rConfig,
                ingestionConfig,
                s3Config,
            );
            // `_removeReaderState` polls the reader on a timer that would
            // outlive the test
            removeReaderState = sinon.stub(populator, '_removeReaderState');
        });

        /**
         * @param {string} zenkoBucket - target zenko bucket of the reader
         * @return {object} the stubbed reader
         */
        function createReaderMock(zenkoBucket) {
            const logReader = sinon.createStubInstance(IngestionReader);
            logReader.getTargetZenkoBucketName.returns(zenkoBucket);
            return logReader;
        }

        it('should unregister a source whose reader is still pending setup',
        () => {
            const logReader = createReaderMock(PENDING_BUCKET);
            populator._ingestionSources[PENDING_BUCKET] = logReader;
            populator.logReadersUpdate = [logReader];

            populator._closeLogState(PENDING_BUCKET);

            assert.deepStrictEqual(populator.logReadersUpdate, []);
            assert.strictEqual(populator._ingestionSources[PENDING_BUCKET],
                undefined);
            // zookeeper state is only created once the setup succeeded
            assert.strictEqual(removeReaderState.called, false);
        });

        it('should unregister an active source and clean its zookeeper state',
        () => {
            const logReader = createReaderMock(ACTIVE_BUCKET);
            populator._ingestionSources[ACTIVE_BUCKET] = logReader;
            populator.logReaders = [logReader];

            populator._closeLogState(ACTIVE_BUCKET);

            assert.deepStrictEqual(populator.logReaders, []);
            assert.strictEqual(populator._ingestionSources[ACTIVE_BUCKET],
                undefined);
            // the reader is read before being unregistered, as
            // `_removeReaderState` polls it until its batch completes
            assert.strictEqual(removeReaderState.calledOnce, true);
            assert.strictEqual(removeReaderState.firstCall.args[0], logReader);
        });

        it('should unregister a source whose setup is still in flight', () => {
            const logReader = createReaderMock(PENDING_BUCKET);
            populator._ingestionSources[PENDING_BUCKET] = logReader;

            populator._closeLogState(PENDING_BUCKET);

            assert.strictEqual(populator._ingestionSources[PENDING_BUCKET],
                undefined);
            assert.strictEqual(removeReaderState.called, false);
        });

        it('should not throw when the source is unknown', () => {
            populator._closeLogState('never-configured-bucket');

            assert.deepStrictEqual(populator._ingestionSources, {});
            assert.strictEqual(removeReaderState.called, false);
        });
    });

    describe('_processLogReaderEntries', () => {
        it('should skip when previous batch currently in progress', () => {
            const logReaderMock = {
                isBatchInProgress: () => true,
                getLocationConstraint: () => 'ring-location',
                getTargetZenkoBucketName: () => 'ring-bucket',
                processLogEntries: sinon.stub().yields(null, false),
            };
            ip._processLogReaderEntries(logReaderMock, {}, err => {
                assert.ifError(err);
                assert(logReaderMock.processLogEntries.notCalled);
            });
        });

        it('should skip when location is paused', () => {
            const logReaderMock = {
                isBatchInProgress: () => false,
                getLocationConstraint: () => 'ring-location',
                getTargetZenkoBucketName: () => 'ring-bucket',
                processLogEntries: sinon.stub().yields(null, false),
            };
            ip.setPausedLocationState('ring-location');
            ip._processLogReaderEntries(logReaderMock, {}, err => {
                assert.ifError(err);
                assert(logReaderMock.processLogEntries.notCalled);
            });
        });

        it('should process log entries', () => {
            const logReaderMock = {
                isBatchInProgress: () => false,
                getLocationConstraint: () => 'ring-location',
                getTargetZenkoBucketName: () => 'ring-bucket',
                processLogEntries: sinon.stub().yields(null, false),
            };
            ip._processLogReaderEntries(logReaderMock, {}, err => {
                assert.ifError(err);
                assert(logReaderMock.processLogEntries.calledOnce);
            });
        });

        it('should skip processing when having more logs but the location is paused', () => {
            const logReaderMock = {
                isBatchInProgress: () => false,
                getLocationConstraint: () => 'ring-location',
                getTargetZenkoBucketName: () => 'ring-bucket',
                processLogEntries: sinon.stub().callsFake((params, cb) => {
                    ip.setPausedLocationState('ring-location');
                    return cb(null, true);
                }),
            };
            ip._processLogReaderEntries(logReaderMock, {}, err => {
                assert.ifError(err);
                assert(logReaderMock.processLogEntries.calledOnce);
            });
        });

        it('should not throw error when processLogEntries fails', () => {
            const logReaderMock = {
                isBatchInProgress: () => false,
                getLocationConstraint: () => 'ring-location',
                getTargetZenkoBucketName: () => 'ring-bucket',
                processLogEntries: sinon.stub().yields(errors.InternalError, false),
            };
            ip._processLogReaderEntries(logReaderMock, {}, err => {
                assert.ifError(err);
                assert(logReaderMock.processLogEntries.calledOnce);
            });
        });

        it('should handle undefined logReader gracefully', done => {
            ip._processLogReaderEntries(undefined, {}, err => {
                assert.ifError(err);
                done();
            });
        });
    });

    describe('_setupProducer producerParams merge', () => {
        let capturedProducerParams;

        beforeEach(() => {
            sinon.stub(BackbeatProducer.prototype, 'setFromConfig').callsFake(function (cfg) {
                capturedProducerParams = cfg.producerParams;
                // Minimal instance state so producerConfig getter doesn't throw.
                this._kafkaHosts = cfg.kafka.hosts;
                this._topic = null;
                this._pollIntervalMs = 2000;
                this._maxRequestSize = 5000020;
                this._compressionType = 'Zstd';
                this._requiredAcks = -1;
                this._producerParams = cfg.producerParams || {};
            });
        });

        afterEach(() => {
            sinon.restore();
            capturedProducerParams = undefined;
        });

        it('should pass merged producerParams : extension overrides global', () => {
            const globalParams = {
                'queue.buffering.max.kbytes': 1048576,
                'queue.buffering.max.ms': 100,
            };
            const extParams = {
                'queue.buffering.max.messages': 200000,
                'queue.buffering.max.ms': 500,
            };

            const populator = new IngestionPopulator(
                null,
                zkConfig,
                { ...kafkaConfig, producerParams: globalParams },
                qpConfig,
                mConfig,
                rConfig,
                { ...ingestionConfig, producerParams: extParams },
                s3Config
            );

            populator._setupProducer(() => {});

            assert.strictEqual(capturedProducerParams['queue.buffering.max.kbytes'], 1048576);
            assert.strictEqual(capturedProducerParams['queue.buffering.max.messages'], 200000);
            assert.strictEqual(capturedProducerParams['queue.buffering.max.ms'], 500,
                'extension producerParams should override global kafka.producerParams');
        });

        it('should work when only global kafka.producerParams are set', () => {
            const globalParams = { 'queue.buffering.max.kbytes': 524288 };

            const populator = new IngestionPopulator(
                null,
                zkConfig,
                { ...kafkaConfig, producerParams: globalParams },
                qpConfig,
                mConfig,
                rConfig,
                ingestionConfig,
                s3Config
            );

            populator._setupProducer(() => {});

            assert.strictEqual(capturedProducerParams['queue.buffering.max.kbytes'], 524288);
        });

        it('should work when only extension producerParams are set', () => {
            const extParams = { 'queue.buffering.max.messages': 100000 };

            const populator = new IngestionPopulator(
                null,
                zkConfig,
                kafkaConfig,
                qpConfig,
                mConfig,
                rConfig,
                { ...ingestionConfig, producerParams: extParams },
                s3Config
            );

            populator._setupProducer(() => {});

            assert.strictEqual(capturedProducerParams['queue.buffering.max.messages'], 100000);
        });
    });
});
