'use strict'; // eslint-disable-line

const assert = require('assert');

const config = require('../../../conf/Config');
const IngestionPopulator =
    require('../../../lib/queuePopulator/IngestionPopulator');
const IngestionReader = require('../../../lib/queuePopulator/IngestionReader');
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
            accessKey: 'myAccessKey',
            secretKey: 'myVerySecretKey',
            endpoint: 'http://127.0.0.1:80',
            bucketName: 'old-ring-bucket',
        },
        locationType: 'location-scality-ring-s3-v1',
    },
};
const existingLocation = {
    'existing-ring': {
        details: {
            accessKey: 'myAccessKey',
            secretKey: 'myVerySecretKey',
            endpoint: 'http://127.0.0.1:8000',
            bucketName: 'existing-ring-bucket',
        },
        locationType: 'location-scality-ring-s3-v1',
    },
};
const newLocation = {
    'new-ring': {
        details: {
            accessKey: 'yourAccessKey',
            secretKey: 'yourVerySecretKey',
            endpoint: 'http://127.0.0.1',
            bucketName: 'new-ring-bucket',
        },
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

class IngestionPopulatorMock extends IngestionPopulator {
    reset() {
        this.clearChanges();
        this._ingestionSources = {};
    }

    clearChanges() {
        this._added = [];
        this._removed = [];
    }

    getAdded() {
        return this._added;
    }

    getRemoved() {
        return this._removed;
    }

    setupMock() {
        // set previous state
        config.setIngestionBuckets(previousLocations, previousBuckets);
        this.applyUpdates();

        // reset for tests
        this.clearChanges();

        // set current state
        config.setIngestionBuckets(currentLocations, currentBuckets);
        this.applyUpdates();
    }

    _setupZkLocationNode(list, cb) {
        // overwrite and ignore creation of zookeeper nodes
        return cb();
    }

    addNewLogSource(newSource) {
        const zenkoBucket = newSource.name;
        this._ingestionSources[zenkoBucket] = new IngestionReader({
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

    before(() => {
        ip = new IngestionPopulatorMock(zkConfig, kafkaConfig, qpConfig,
            mConfig, rConfig, ingestionConfig, s3Config);
        ip.reset();
    });

    beforeEach(() => {
        ip.setupMock();
        ip.applyUpdates();
    });

    afterEach(() => {
        ip.reset();
    });

    it('should fetch correctly formed ingestion bucket object information',
    () => {
        const buckets = config.getIngestionBuckets();
        buckets.forEach(bucket => {
            assert(bucket.accessKey);
            assert(bucket.secretKey);
            assert(bucket.endpoint);
            assert(bucket.locationType);
            assert.strictEqual(bucket.locationType, 'scality_s3');
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
                newSource.name === NEW_BUCKET);
            assert.equal(source.port, 80);
        });

        it('should keep an existing active ingestion source', () => {
            const wasAdded = ip.getAdded().findIndex(r =>
                r.name === EXISTING_BUCKET) >= 0;
            const wasRemoved = ip.getRemoved().findIndex(r =>
                r === EXISTING_BUCKET) >= 0;

            assert(!wasAdded);
            assert(!wasRemoved);
        });

        it('should add a new ingestion source', () => {
            const wasAdded = ip.getAdded().findIndex(r =>
                r.name === NEW_BUCKET) >= 0;
            const wasRemoved = ip.getRemoved().findIndex(r =>
                r === NEW_BUCKET) >= 0;

            assert(wasAdded);
            assert(!wasRemoved);
        });

        it('should remove an ingestion source that is has become inactive',
        () => {
            const wasAdded = ip.getAdded().findIndex(r =>
                r.name === OLD_BUCKET) >= 0;
            const wasRemoved = ip.getRemoved().findIndex(r =>
                r === OLD_BUCKET) >= 0;

            assert(!wasAdded);
            assert(wasRemoved);
        });

        it('should update an ingestion reader when the ingestion source ' +
        'information is updated', () => {
            ip.clearChanges();

            assert.strictEqual(Object.keys(ip._ingestionSources).length, 3);

            // hack to update a valid editable field
            const updatedKeyName = 'anUpdatedKey';
            const locationName = Object.keys(existingLocation)[0];
            const dupeExistingLoc = Object.assign({}, existingLocation);
            dupeExistingLoc[locationName].details.accessKey = updatedKeyName;
            config.setIngestionBuckets(dupeExistingLoc, [existingBucket]);

            ip.applyUpdates();

            const added = ip.getAdded();
            assert.strictEqual(added.length, 1);
            assert.strictEqual(added[0].locationConstraint, locationName);
            assert.deepStrictEqual(added[0].auth.accessKey, updatedKeyName);
        });
    });
});
