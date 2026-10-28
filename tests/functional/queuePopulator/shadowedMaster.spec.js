const assert = require('assert');
const { MongoClient, BSON: { EJSON } } = require('mongodb');
const { Logger } = require('werelogs');
const {
    S3Client,
    CreateBucketCommand,
    PutBucketVersioningCommand,
    PutBucketNotificationConfigurationCommand,
    PutObjectCommand,
    PutObjectTaggingCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');

const { isMasterKey } = require('@scality/arsenal').versioning;
const LogReader = require('../../../lib/queuePopulator/LogReader');
const ShadowedMasterFilter = require('../../../lib/queuePopulator/ShadowedMasterFilter');
const ListRecordStream = require('../../../lib/queuePopulator/KafkaLogConsumer/ListRecordStream');
const MultipleBucketsPipelineFactory =
    require('../../../extensions/oplogPopulator/pipeline/MultipleBucketsPipelineFactory');

const logger = new Logger('test:shadowedMaster');

const s3 = new S3Client({
    endpoint: 'http://127.0.0.1:8000',
    forcePathStyle: true,
    region: 'us-east-1',
    credentials: { accessKeyId: 'accessKey1', secretAccessKey: 'verySecretKey1' },
});

const MONGO_URL = 'mongodb://127.0.0.1:27018/?replicaSet=rs0&directConnection=true';
const DATABASE = 'metadata';
// the change stream is deemed drained after this long without an event
const QUIET_MS = 1500;

/**
 * Entries the populator extensions receive for one bucket, as the kafka log
 * reader would dispatch them: the bucket collection is watched with the
 * pipeline the oplog populator installs on the connector, each event is
 * serialized like the connector does and fed to the reader.
 */
class ObservedBucket {
    constructor(mongo, bucket) {
        this.bucket = bucket;
        this.entries = [];
        this._stream = new ListRecordStream(logger);
        this._reader = new LogReader({
            logId: 'test',
            logger,
            extensions: [{ filter: entry => this.entries.push(entry) }],
            shadowedMasterFilter: new ShadowedMasterFilter(),
        });
        const pipeline = JSON.parse(new MultipleBucketsPipelineFactory(0).getPipeline([bucket]));
        this._changeStream = mongo.db(DATABASE).collection(bucket).watch(pipeline);
        this._changeStream.on('change', event => this._stream.write({
            value: Buffer.from(EJSON.stringify(event)),
            timestamp: Date.now(),
        }));
        this._stream.on('data', record => {
            this._lastEvent = Date.now();
            this._reader._processLogEntry({}, record, record.entries[0], () => {});
        });
        this._lastEvent = Date.now();
    }

    async create() {
        await s3.send(new CreateBucketCommand({ Bucket: this.bucket }));
        // a delete only carries the object metadata when a consumer needs
        // it, i.e. for an archived object or with notifications configured:
        // the latter is the cheap way to see which key a delete touches
        await s3.send(new PutBucketNotificationConfigurationCommand({
            Bucket: this.bucket,
            NotificationConfiguration: {
                QueueConfigurations: [{
                    QueueArn: 'arn:scality:bucketnotif:::destination1',
                    Events: ['s3:ObjectRemoved:*'],
                }],
            },
        }));
    }

    async setVersioning(status) {
        await s3.send(new PutBucketVersioningCommand({
            Bucket: this.bucket, VersioningConfiguration: { Status: status },
        }));
    }

    async put(key, body = 'data') {
        const res = await s3.send(new PutObjectCommand({ Bucket: this.bucket, Key: key, Body: body }));
        return res.VersionId;
    }

    async tag(key, versionId, value) {
        await s3.send(new PutObjectTaggingCommand({
            Bucket: this.bucket, Key: key, VersionId: versionId,
            Tagging: { TagSet: [{ Key: 'k', Value: value }] },
        }));
    }

    async delete(key, versionId) {
        await s3.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: key, VersionId: versionId }));
    }

    /**
     * Entries dispatched since the last call, once the stream is quiet.
     *
     * @return {Object[]} entries, summarized as { type, key, isNull, isDeleteMarker }
     */
    async drain() {
        // quiet time starts at the call, so an event still in flight is awaited
        const start = Date.now();
        for (;;) {
            const quietFor = Date.now() - Math.max(this._lastEvent, start);
            if (quietFor >= QUIET_MS) {
                break;
            }
            await new Promise(resolve => setTimeout(resolve, QUIET_MS - quietFor));
        }
        this.drained = this.entries.splice(0);
        return this.drained.map(({ type, key, value }) => {
            const md = JSON.parse(value);
            return {
                type,
                key: isMasterKey(key) ? 'master' : 'version',
                isNull: !!md.isNull,
                isDeleteMarker: !!md.isDeleteMarker,
                isPHD: !!md.isPHD,
            };
        });
    }

    async close() {
        await this._changeStream.close();
    }
}

const master = (type = 'put', flags = {}) =>
    ({ type, key: 'master', isNull: false, isDeleteMarker: false, isPHD: false, ...flags });
const version = (type = 'put', flags = {}) =>
    ({ type, key: 'version', isNull: false, isDeleteMarker: false, isPHD: false, ...flags });

// Every layout of a master and its version keys the MongoDB backend writes,
// and the entries the extensions receive for it after the shadowed-master
// filter. The layouts are those of
// https://scality.atlassian.net/wiki/spaces/~88394258/pages/4305551381
(process.env.PROFILE === 'mongo' ? describe : describe.skip)('Shadowed master filter on the MongoDB change stream',
function suite() {
    this.timeout(60000);

    let mongo;
    let bucket;
    let counter = 0;

    before(async () => {
        mongo = await MongoClient.connect(MONGO_URL);
    });

    after(async () => {
        await mongo.close();
    });

    beforeEach(async () => {
        counter += 1;
        bucket = new ObservedBucket(mongo, `shadowed-master-${Date.now()}-${counter}`);
        await bucket.create();
        await bucket.drain();
    });

    afterEach(async () => {
        await bucket.close();
    });

    it('put in a non-versioned bucket', async () => {
        await bucket.put('obj');
        assert.deepStrictEqual(await bucket.drain(), [master()]);
    });

    it('delete in a non-versioned bucket', async () => {
        await bucket.put('obj');
        await bucket.drain();
        await bucket.delete('obj');
        assert.deepStrictEqual(await bucket.drain(), [master('delete')]);
    });

    it('metadata update of an object put before versioning was enabled', async () => {
        await bucket.put('obj');
        await bucket.setVersioning('Enabled');
        await bucket.drain();
        // the object still lives in its master key only, with no version id
        await bucket.tag('obj', undefined, 'v1');
        assert.deepStrictEqual(await bucket.drain(), [master()]);
    });

    it('put in a versioned bucket', async () => {
        await bucket.setVersioning('Enabled');
        await bucket.put('obj');
        // the master is a copy of the version too, but the extensions skip
        // it on their own: the filter only acts on null versions
        assert.deepStrictEqual(await bucket.drain(), [version(), master()]);
    });

    it('new version on top of another', async () => {
        await bucket.setVersioning('Enabled');
        await bucket.put('obj');
        await bucket.drain();
        await bucket.put('obj');
        assert.deepStrictEqual(await bucket.drain(), [version(), master()]);
    });

    it('delete marker in a versioned bucket', async () => {
        await bucket.setVersioning('Enabled');
        await bucket.put('obj');
        await bucket.drain();
        await bucket.delete('obj');
        assert.deepStrictEqual(await bucket.drain(), [version('put', { isDeleteMarker: true })]);
    });

    it('put in a suspended bucket', async () => {
        await bucket.setVersioning('Suspended');
        await bucket.put('obj');
        assert.deepStrictEqual(await bucket.drain(), [master('put', { isNull: true })]);
    });

    it('delete marker in a suspended bucket', async () => {
        await bucket.setVersioning('Suspended');
        await bucket.put('obj');
        await bucket.drain();
        await bucket.delete('obj');
        // the null master is turned into a placeholder, deleted with the
        // null version's metadata, then replaced by the delete marker
        assert.deepStrictEqual(await bucket.drain(), [
            master('delete', { isNull: true, isPHD: true }),
            master('put', { isNull: true, isDeleteMarker: true }),
        ]);
    });

    it('metadata update of a non-current null version', async () => {
        await bucket.setVersioning('Suspended');
        await bucket.put('obj');
        await bucket.setVersioning('Enabled');
        await bucket.put('obj');
        await bucket.drain();
        await bucket.tag('obj', 'null', 'v1');
        // the null version lives in a version key of its own, named after
        // its internal version id: MongoDB does not use the null key (empty
        // version id) S3C stores it under. Should ARSN-632 bring the null
        // key to MongoDB, the entry would still be a version key
        assert.deepStrictEqual(await bucket.drain(), [version('put', { isNull: true })]);
        assert.deepStrictEqual(bucket.drained.filter(e => e.key.endsWith('\x00')), []);
    });

    it('delete of a non-current null version', async () => {
        await bucket.setVersioning('Suspended');
        await bucket.put('obj');
        await bucket.setVersioning('Enabled');
        await bucket.put('obj');
        await bucket.drain();
        await bucket.delete('obj', 'null');
        // only its version key is touched, so GC must process it
        assert.deepStrictEqual(await bucket.drain(), [version('delete', { isNull: true })]);
    });

    it('metadata update of a current null version', async () => {
        await bucket.setVersioning('Suspended');
        await bucket.put('obj');
        await bucket.drain();
        await bucket.tag('obj', 'null', 'v1');
        // the master is copied into a version key, which is processed, and
        // the master itself dropped
        assert.deepStrictEqual(await bucket.drain(), [version('put', { isNull: true })]);
    });

    it('identical metadata update of a current null version', async () => {
        await bucket.setVersioning('Suspended');
        await bucket.put('obj');
        await bucket.tag('obj', 'null', 'v1');
        await bucket.drain();
        await bucket.tag('obj', 'null', 'v1');
        // a no-op write emits no event, so the twin pair is the only way to
        // see the same master value twice in a row
        assert.deepStrictEqual(await bucket.drain(), []);
    });

    it('delete of a current null version copied into a version key', async () => {
        await bucket.setVersioning('Suspended');
        await bucket.put('obj');
        await bucket.tag('obj', 'null', 'v1');
        await bucket.drain();
        await bucket.delete('obj', 'null');
        // the version key is deleted with the null version's metadata, then
        // the master, turned into a placeholder with a version id of its own
        // and dropped as the placeholder of the null version just deleted
        assert.deepStrictEqual(await bucket.drain(), [version('delete', { isNull: true })]);
    });

    it('delete of a non-current null version then of the current version', async () => {
        await bucket.setVersioning('Suspended');
        await bucket.put('obj');
        await bucket.setVersioning('Enabled');
        const current = await bucket.put('obj');
        await bucket.drain();
        await bucket.delete('obj', 'null');
        await bucket.delete('obj', current);
        // the placeholder left by the last version is that of the current
        // version, not of the null version deleted just before
        assert.deepStrictEqual(await bucket.drain(), [
            version('delete', { isNull: true }),
            version('delete'),
        ]);
    });

    it('delete of the current version with older versions', async () => {
        await bucket.setVersioning('Enabled');
        await bucket.put('obj');
        const current = await bucket.put('obj');
        await bucket.drain();
        await bucket.delete('obj', current);
        // the placeholder set on the master carries no value and yields no
        // entry; the later repair rewrites the master as a shadowed one
        assert.deepStrictEqual(await bucket.drain(), [version('delete')]);
    });
});
