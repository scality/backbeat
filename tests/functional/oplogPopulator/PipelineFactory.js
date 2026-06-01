const assert = require('assert');
const MongoClient = require('mongodb').MongoClient;

const testConfig = require('../../config.json');
const MultipleBucketsPipelineFactory =
    require('../../../extensions/oplogPopulator/pipeline/MultipleBucketsPipelineFactory');

const mongoUrl =
    `mongodb://${testConfig.queuePopulator.mongo.replicaSetHosts}` +
    '/db?replicaSet=rs0';
const client = new MongoClient(mongoUrl, {});
const db = client.db('metadata', { ignoreUndefined: true });

const THRESHOLD = 100;

describe('PipelineFactory', function () {
    this.timeout(30000);

    const collectionName = 'test-pipeline-stripping';
    let collection;
    let setStage;
    let addFieldsStage;

    before(async () => {
        await client.connect();
        collection = db.collection(collectionName);

        const factory = new MultipleBucketsPipelineFactory(THRESHOLD);
        const pipeline = JSON.parse(factory.getPipeline(['test-bucket']));
        addFieldsStage = pipeline[1];
        setStage = pipeline[2];
    });

    afterEach(async () => {
        await collection.deleteMany({});
    });

    after(async () => {
        if (collection) {
            await collection.drop().catch(() => {});
        }
        await client.close();
    });

    describe('fullDocument path (insert/replace events)', () => {
        it('should preserve location when content-length is missing', async () => {
            const doc = { fullDocument: { value: { location: [{ key: 'a' }] } } };
            await collection.insertOne(doc);
            const results = await collection.aggregate([setStage]).toArray();
            assert.strictEqual(results.length, 1);
            assert.deepStrictEqual(results[0].fullDocument.value.location, [{ key: 'a' }]);
        });

        it('should preserve location when content-length is below threshold', async () => {
            const doc = { fullDocument: { value: { 'content-length': 50, 'location': [{ key: 'a' }] } } };
            await collection.insertOne(doc);
            const results = await collection.aggregate([setStage]).toArray();
            assert.strictEqual(results.length, 1);
            assert.deepStrictEqual(results[0].fullDocument.value.location, [{ key: 'a' }]);
        });

        it('should preserve location when content-length is zero', async () => {
            const doc = { fullDocument: { value: { 'content-length': 0, 'location': [{ key: 'a' }] } } };
            await collection.insertOne(doc);
            const results = await collection.aggregate([setStage]).toArray();
            assert.strictEqual(results.length, 1);
            assert.deepStrictEqual(results[0].fullDocument.value.location, [{ key: 'a' }]);
        });

        it('should strip location when content-length equals threshold', async () => {
            const doc = { fullDocument: { value: { 'content-length': THRESHOLD, 'location': [{ key: 'a' }] } } };
            await collection.insertOne(doc);
            const results = await collection.aggregate([setStage]).toArray();
            assert.strictEqual(results.length, 1);
            assert.strictEqual(results[0].fullDocument.value.location, undefined);
        });

        it('should strip location when content-length is above threshold', async () => {
            const doc = { fullDocument: { value: { 'content-length': 200, 'location': [{ key: 'a' }] } } };
            await collection.insertOne(doc);
            const results = await collection.aggregate([setStage]).toArray();
            assert.strictEqual(results.length, 1);
            assert.strictEqual(results[0].fullDocument.value.location, undefined);
        });
    });

    describe('updateDescription path (update events)', () => {
        it('should preserve location when content-length is missing', async () => {
            const doc = { updateDescription: { updatedFields: { value: { location: [{ key: 'a' }] } } } };
            await collection.insertOne(doc);
            const results = await collection.aggregate([setStage]).toArray();
            assert.strictEqual(results.length, 1);
            assert.deepStrictEqual(
                results[0].updateDescription.updatedFields.value.location,
                [{ key: 'a' }]
            );
        });

        it('should preserve location when content-length is below threshold', async () => {
            const doc = {
                updateDescription: { updatedFields: { value: { 'content-length': 50, 'location': [{ key: 'a' }] } } },
            };
            await collection.insertOne(doc);
            const results = await collection.aggregate([setStage]).toArray();
            assert.strictEqual(results.length, 1);
            assert.deepStrictEqual(
                results[0].updateDescription.updatedFields.value.location,
                [{ key: 'a' }]
            );
        });

        it('should preserve location when content-length is zero', async () => {
            const doc = {
                updateDescription: { updatedFields: { value: { 'content-length': 0, 'location': [{ key: 'a' }] } } },
            };
            await collection.insertOne(doc);
            const results = await collection.aggregate([setStage]).toArray();
            assert.strictEqual(results.length, 1);
            assert.deepStrictEqual(
                results[0].updateDescription.updatedFields.value.location,
                [{ key: 'a' }]
            );
        });

        it('should strip location when content-length equals threshold', async () => {
            const value = { 'content-length': THRESHOLD, 'location': [{ key: 'a' }] };
            const doc = {
                updateDescription: { updatedFields: { value } },
            };
            await collection.insertOne(doc);
            const results = await collection.aggregate([setStage]).toArray();
            assert.strictEqual(results.length, 1);
            assert.strictEqual(
                results[0].updateDescription.updatedFields.value.location,
                undefined
            );
        });

        it('should strip location when content-length is above threshold', async () => {
            const doc = {
                updateDescription: { updatedFields: { value: { 'content-length': 200, 'location': [{ key: 'a' }] } } },
            };
            await collection.insertOne(doc);
            const results = await collection.aggregate([setStage]).toArray();
            assert.strictEqual(results.length, 1);
            assert.strictEqual(
                results[0].updateDescription.updatedFields.value.location,
                undefined
            );
        });
    });

    describe('key synthesis ($addFields)', () => {
        it('should populate key from fullDocument.value.key on insert-shaped docs', async () => {
            const doc = { fullDocument: { value: { key: 'my/object' } } };
            await collection.insertOne(doc);
            const results = await collection.aggregate([addFieldsStage]).toArray();
            assert.strictEqual(results.length, 1);
            assert.strictEqual(results[0].key, 'my/object');
        });

        it('should populate key from updateDescription.updatedFields.value.key on update-shaped docs', async () => {
            const doc = { updateDescription: { updatedFields: { value: { key: 'my/object' } } } };
            await collection.insertOne(doc);
            const results = await collection.aggregate([addFieldsStage]).toArray();
            assert.strictEqual(results.length, 1);
            assert.strictEqual(results[0].key, 'my/object');
        });

        it('should prefer fullDocument.value.key when both are present', async () => {
            const doc = {
                fullDocument: { value: { key: 'from-full' } },
                updateDescription: { updatedFields: { value: { key: 'from-update' } } },
            };
            await collection.insertOne(doc);
            const results = await collection.aggregate([addFieldsStage]).toArray();
            assert.strictEqual(results[0].key, 'from-full');
        });

        it('should leave key absent when neither path is populated', async () => {
            // $ifNull returns missing (not null) when all inputs are missing.
            // The connector's nullable Avro key schema emits this as null on
            // the wire, so the partition outcome is the same as an explicit
            // null. In production deletes are ignored, so this case doesn't
            // occur for consumed events.
            const doc = { fullDocument: null };
            await collection.insertOne(doc);
            const results = await collection.aggregate([addFieldsStage]).toArray();
            assert.strictEqual(results.length, 1);
            assert.strictEqual(results[0].key, undefined);
        });
    });
});
