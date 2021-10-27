'use strict'; // eslint-disable-line

const assert = require('assert');
const config = require('../../../lib/Config');
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
});
