const assert = require('assert');
const { MongoClient } = require('mongodb');

const testConfig = require('../../config.json');
const HashedPipelineFactory = require('../../../extensions/oplogPopulator/pipeline/HashedPipelineFactory');

const mongoUrl =
    `mongodb://${testConfig.queuePopulator.mongo.replicaSetHosts}` +
    '/db?replicaSet=rs0';

const nbConnectors = 3;
const buckets = [...Array(30).keys()].map(i => `hashed-bucket-${i}`);
const excluded = ['PENSIEVE', '__metastore', 'mpuShadowBuckethashed-bucket-0'];

async function collect(streams, expected, timeoutMs) {
    const events = streams.map(() => []);
    const deadline = Date.now() + timeoutMs;
    while (events.flat().length < expected && Date.now() < deadline) {
        await Promise.all(streams.map(async (stream, index) => {
            const event = await stream.tryNext();
            if (event) {
                events[index].push(event);
            }
        }));
    }
    // let any unexpected event come through
    await Promise.all(streams.map(async (stream, index) => {
        let event = await stream.tryNext();
        while (event) {
            events[index].push(event);
            event = await stream.tryNext();
        }
    }));
    return events;
}

describe('HashedPipelineFactory', function () {
    this.timeout(60000);

    const client = new MongoClient(mongoUrl, {});
    const db = client.db('hashedPipelineTest', { ignoreUndefined: true });
    let streams;

    before(async () => {
        await client.connect();
        await db.dropDatabase();
        const { operationTime } = await db.command({ ping: 1 });
        const factory = new HashedPipelineFactory(0);
        streams = [...Array(nbConnectors).keys()].map(index => db.watch(
            JSON.parse(factory.getPipeline({ nbConnectors, index })),
            { startAtOperationTime: operationTime }));
    });

    after(async () => {
        await Promise.all(streams.map(stream => stream.close()));
        await db.dropDatabase();
        await client.close();
    });

    it('should route each bucket to exactly one connector, and nothing else', async () => {
        for (const bucket of buckets) {
            await db.collection(bucket).insertOne({ _id: 'key', value: { key: 'key' } });
        }
        await db.collection(buckets[0]).updateOne({ _id: 'key' }, { $set: { value: { key: 'key2' } } });
        await db.collection(buckets[0]).deleteOne({ _id: 'key' });
        for (const name of excluded) {
            await db.collection(name).insertOne({ _id: 'key', value: { key: 'key' } });
        }
        await db.createCollection('hashed-ddl');
        await db.collection('hashed-ddl').drop();

        const events = await collect(streams, buckets.length + 1, 30000);

        const connectorOf = new Map();
        events.forEach((connectorEvents, index) => connectorEvents.forEach(event => {
            const coll = event.ns.coll;
            assert(!excluded.includes(coll), `${coll} should be excluded`);
            assert(['insert', 'update'].includes(event.operationType),
                `${event.operationType} event should be filtered`);
            assert.strictEqual(connectorOf.get(coll) ?? index, index, `${coll} spread over connectors`);
            connectorOf.set(coll, index);
        }));
        assert.deepStrictEqual([...connectorOf.keys()].sort(), [...buckets].sort());
        assert.strictEqual(events.flat().length, buckets.length + 1);
        events.forEach((connectorEvents, index) =>
            assert(connectorEvents.length > 0, `connector ${index} got no bucket`));
    });
});
