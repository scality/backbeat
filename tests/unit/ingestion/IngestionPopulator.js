'use strict'; // eslint-disable-line

const assert = require('assert');

const config = require('../../../conf/Config');
const IngestionPopulator =
    require('../../../lib/queuePopulator/IngestionPopulator');

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

const locations = [
    {
        locationConstraint: 'my-ring',
        zenkoBucket: EXISTING_BUCKET,
        ingestion: { Status: 'enabled' },
        sourceBucket: 'my-ring-bucket',
        accessKey: 'myAccessKey',
        secretKey: 'myVerySecretKey',
        endpoint: 'http://127.0.0.1:8000',
        locationType: 'scality_s3',
    },
    {
        locationConstraint: 'your-ring',
        zenkoBucket: NEW_BUCKET,
        ingestion: { Status: 'enabled' },
        sourceBucket: 'your-ring-bucket',
        accessKey: 'yourAccessKey',
        secretKey: 'yourVerySecretKey',
        endpoint: 'http://127.0.0.1:8000',
        locationType: 'scality_s3',
    },
];

class MongoMock {
    getIngestionBuckets(cb) {
        const bucketList = locations.map(l => ({
            name: l.zenkoBucket,
            ingestion: l.ingestion,
            locationConstraint: l.locationConstraint,
        }));
        return cb(null, bucketList);
    }
}

class IngestionPopulatorMock extends IngestionPopulator {
    reset() {
        this._added = [];
        this._removed = [];
    }

    getAdded() {
        return this._added;
    }

    getRemoved() {
        return this._removed;
    }

    _mockLocationDetails() {
        const locationDetails = {};
        locations.forEach(l => {
            locationDetails[l.locationConstraint] = {
                accessKey: l.accessKey,
                secretKey: l.secretKey,
                endpoint: l.endpoint,
                locationType: l.locationType,
                bucketName: l.sourceBucket,
            };
        });
        return locationDetails;
    }

    setupMock() {
        // for testing purposes
        this._added = [];
        this._removed = [];

        // mocks
        this._mongoClient = new MongoMock();
        this._extension = {
            createZkPath: cb => cb(),
        };
        this._locationDetails = this._mockLocationDetails();

        // mock existing provisioned sources
        this._provisionedIngestionSources = {
            [OLD_BUCKET]: {},
            [EXISTING_BUCKET]: {},
        };
    }

    addNewLogSource(newSource, cb) {
        this._added.push(newSource);
        return cb();
    }

    closeLogState(source, cb) {
        this._removed.push(source);
        return cb();
    }
}

describe('Ingestion Populator', () => {
    describe('applyUpdates helper method', () => {
        let ip;

        before(() => {
            ip = new IngestionPopulatorMock(zkConfig, kafkaConfig, qpConfig,
                mConfig, rConfig, ingestionConfig, s3Config);
            ip.setupMock();
        });

        beforeEach(done => {
            ip.applyUpdates(done);
        });

        afterEach(() => {
            ip.reset();
        });

        it('should attach configuration properties for each new source', () => {
            ip.getAdded().forEach(newSource => {
                // default configs
                assert.strictEqual(newSource.cronRule, '*/5 * * * * *');
                assert.strictEqual(newSource.zookeeperSuffix,
                    `/${newSource.name}`);
                assert.strictEqual(newSource.raftCount, 8);

                // unique configs
                assert(newSource.name);
                assert(newSource.bucket);
                assert(newSource.prefix);
                assert(newSource.host);
                assert.strictEqual(typeof newSource.port, 'number');
                assert.strictEqual(typeof newSource.https, 'boolean');
                assert(newSource.type);
            });
        });

        it('should keep an existing provisioned source', () => {
            const inAdd = ip.getAdded().findIndex(r =>
                r.name === EXISTING_BUCKET);
            const inRemove = ip.getRemoved().findIndex(r =>
                r === EXISTING_BUCKET);

            assert(inAdd === -1);
            assert(inRemove === -1);
        });

        it('should add/provision a new ingestion bucket', () => {
            const inAdd = ip.getAdded().findIndex(r =>
                r.name === NEW_BUCKET);
            const inRemove = ip.getRemoved().findIndex(r =>
                r === NEW_BUCKET);
            assert(inAdd >= 0);
            assert(inRemove === -1);
        });

        it('should remove a provisioned source that is no longer an ' +
        'ingestion bucket', () => {
            const inAdd = ip.getAdded().findIndex(r =>
                r.name === OLD_BUCKET);
            const inRemove = ip.getRemoved().findIndex(r =>
                r === OLD_BUCKET);

            assert(inAdd === -1);
            assert(inRemove >= 0);
        });
    });
});
