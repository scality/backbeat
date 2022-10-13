const assert = require('assert');
const werelogs = require('werelogs');
const sinon = require('sinon');
const events = require('events');
const MongoClient = require('mongodb').MongoClient;

const NotificationConfigManager
    = require('../../../extensions/notification/NotificationConfigManager');
const mongoConfig
    = require('../../config.json').queuePopulator.mongo;

const logger = new werelogs.Logger('NotificationConfigManager:test');

const notificationConfiguration = {
    queueConfig: [
        {
            events: ['s3:ObjectCreated:Put'],
            queueArn: 'arn:scality:bucketnotif:::destination1',
            filterRules: [],
        },
    ],
};

const notificationConfigurationVariant = {
    queueConfig: [
        {
            events: ['s3:ObjectCreated:*'],
            queueArn: 'arn:scality:bucketnotif:::destination2',
            filterRules: [],
        },
    ],
};

describe('NotificationConfigManager ::', () => {
    const params = {
        mongoConfig,
        logger,
    };
    let manager;
    beforeEach(done => {
        // Initializing a NotificationConfigManager instance
        manager = new NotificationConfigManager(params);
        const getCollectionStub = sinon.stub().returns({
            // mock change stream
            watch: () => new events.EventEmitter(),
            // mock bucket notification configuration
            findOne: () => (
                {
                    value: {
                        notificationConfiguration,
                    }
                }),
        });
        const mongoCommandStub = sinon.stub().returns({
            version: '4.3.17',
        });
        const getDbStub = sinon.stub().returns({
            collection: getCollectionStub,
            command: mongoCommandStub,
        });
        sinon.stub(MongoClient, 'connect').callsArgWith(2, null, {
            db: getDbStub,
        });
        manager.setup(done);
    });

    afterEach(() => {
        sinon.restore();
    });

    it('getConfig should return correct value', async () => {
        // should store new configs in cache
        const config = await manager.getConfig('example-bucket-1');
        assert.deepEqual(config, notificationConfiguration);
    });

    it('Cache should store new configs', async () => {
        // cache should initially be empty
        assert.strictEqual(manager._cachedConfigs.count(), 0);
        // should store new configs in cache
        await manager.getConfig('example-bucket-1');
        assert.strictEqual(manager._cachedConfigs.count(), 1);
        assert.deepEqual(manager._cachedConfigs.get('example-bucket-1'),
            notificationConfiguration);
        await manager.getConfig('example-bucket-2');
        assert.deepEqual(manager._cachedConfigs.get('example-bucket-2'),
            notificationConfiguration);
        assert.strictEqual(manager._cachedConfigs.count(), 2);
        // should retreive config fom cache without re-adding it
        await manager.getConfig('example-bucket-1');
        assert.strictEqual(manager._cachedConfigs.count(), 2);
    });

    it('Cache should be invalidated when change stream event occurs (delete event)',
        async () => {
        // adding configs to cache
        await manager.getConfig('example-bucket-1');
        await manager.getConfig('example-bucket-2');
        // pushing a new delete event to the change stream
        const changeStreamEvent = {
            _id: 'resumeToken',
            operationType: 'delete',
            documentKey: {
                _id: 'example-bucket-1',
            },
        };
        manager._metastoreChangeStream._changeStream.emit('change', changeStreamEvent);
        // cached config for "example-bucket-1" should be invalidated
        assert.strictEqual(manager._cachedConfigs.count(), 1);
        assert.strictEqual(manager._cachedConfigs.get('example-bucket-1'), undefined);
        assert(manager._cachedConfigs.get('example-bucket-2'));
    });

    it('Cache should be invalidated when change stream event occurs (replace/update events)',
        async () => {
        // adding configs to cache
        await manager.getConfig('example-bucket-1');
        await manager.getConfig('example-bucket-2');
        // pushing a new replace event to the change stream
        const changeStreamEvent = {
            _id: 'resumeToken',
            operationType: 'replace',
            documentKey: {
                _id: 'example-bucket-1',
            },
            fullDocument: {
                _id: 'example-bucket-1',
                value: {
                    notificationConfiguration: notificationConfigurationVariant,
                }
            }
        };
        manager._metastoreChangeStream._changeStream.emit('change', changeStreamEvent);
        // cached config for "example-bucket-1" should be invalidated
        assert.strictEqual(manager._cachedConfigs.count(), 2);
        assert.deepEqual(manager._cachedConfigs.get('example-bucket-1'),
            notificationConfigurationVariant);
        // update event should yield the same results
        changeStreamEvent.operationType = 'update';
        changeStreamEvent.fullDocument._id = 'example-bucket-2';
        manager._metastoreChangeStream._changeStream.emit('change', changeStreamEvent);
        assert.strictEqual(manager._cachedConfigs.count(), 2);
        assert.deepEqual(manager._cachedConfigs.get('example-bucket-2'),
            notificationConfiguration);
    });
});
