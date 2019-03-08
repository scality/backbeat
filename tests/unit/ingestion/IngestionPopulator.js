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

const buckets = [
    {
        locationConstraint: 'my-ring',
        zenkoBucket: EXISTING_BUCKET,
        ingestion: { Status: 'enabled' },
        bucketName: 'my-ring-bucket',
        accessKey: 'myAccessKey',
        secretKey: 'myVerySecretKey',
        endpoint: 'http://127.0.0.1:8000',
        locationType: 'scality_s3',
    },
    {
        locationConstraint: 'your-ring',
        zenkoBucket: NEW_BUCKET,
        ingestion: { Status: 'enabled' },
        bucketName: 'your-ring-bucket',
        accessKey: 'yourAccessKey',
        secretKey: 'yourVerySecretKey',
        endpoint: 'http://127.0.0.1',
        locationType: 'scality_s3',
    },
];

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

    setupMock() {
        // for testing purposes
        this._added = [];
        this._removed = [];

        // mocks
        this._extension = {
            createZkPath: cb => cb(),
        };

        // mock existing active sources
        this._activeIngestionSources = {
            [OLD_BUCKET]: {},
            [EXISTING_BUCKET]: {},
        };
    }

    addNewLogSource(newSource) {
        this._added.push(newSource);
    }

    closeLogState(source) {
        this._removed.push(source);
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

        beforeEach(() => {
            ip.applyUpdates(buckets);
        });

        afterEach(() => {
            ip.reset();
        });

        it('should attach configuration properties for each new ingestion ' +
        'source', () => {
            ip.getAdded().forEach(newSource => {
                assert(newSource.name);
                assert(newSource.bucket);
                assert(newSource.host);
                assert.strictEqual(typeof newSource.port, 'number');
                assert.strictEqual(typeof newSource.https, 'boolean');
                assert(newSource.type);
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
    });
});
