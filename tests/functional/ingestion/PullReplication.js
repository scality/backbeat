'use strict';

const assert = require('assert');
const { promisify } = require('util');
const sinon = require('sinon');
const { ObjectMD, BucketInfo } = require('arsenal').models;
const { VersionID } = require('arsenal').versioning;
const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

const config = require('../../config.json');
const MongoQueueProcessor =
    require('../../../extensions/mongoProcessor/MongoQueueProcessor');
const fakeLogger = require('../../utils/fakeLogger');

const REPLICATION_GROUP_ID = 'RG001';
const LOCATION = 'us-east-1';
const COLD_LOCATION = 'cold-location';
const KEY = 'pull-replication-key';

// the mock client hands back the very document it was given, so what it
// cannot show is how mongo lays versions out and what it gives back
describe('MongoQueueProcessor in dr mode against mongodb', function drMongo() {
    this.timeout(30000);

    let mqp;
    let mongoClient;
    let bucket;
    let bucketId = 0;

    before(async () => {
        mqp = new MongoQueueProcessor(config.kafka,
            { ...config.extensions.mongoProcessor, mode: 'dr' },
            {
                ...config.queuePopulator.mongo,
                replicaSet: 'rs0',
                writeConcern: 'majority',
                database: `pullreplication${Date.now()}`,
                replicationGroupId: REPLICATION_GROUP_ID,
            },
            {});
        mqp._mProducer = { publishMetrics: () => {}, close: () => {} };
        mqp._bootstrapList = [];
        mongoClient = mqp._mongoClient;
        await promisify(mongoClient.setup.bind(mongoClient))();
    });

    after(async () => {
        await mongoClient.db.dropDatabase();
        await promisify(mongoClient.close.bind(mongoClient))();
    });

    beforeEach(async () => {
        bucketId += 1;
        bucket = `pull-replication-bucket-${bucketId}`;
        const bucketInfo = new BucketInfo(bucket, 'owner-id', 'owner',
            new Date().toJSON(), BucketInfo.currentModelVersion());
        bucketInfo.setLocationConstraint(LOCATION);
        bucketInfo.setVersioningConfiguration({ Status: 'Enabled' });
        await promisify(mongoClient.createBucket.bind(mongoClient))(
            bucket, bucketInfo, fakeLogger);
    });

    afterEach(() => sinon.restore());

    function coldObject(versionId, fields = {}) {
        const objmd = new ObjectMD()
            .setKey(KEY)
            .setContentLength(1024)
            .setContentMd5('9e107d9d372bb6826bd81d3542a419d6')
            .setLastModified('2026-09-01T10:00:00.000Z')
            .setAmzStorageClass(COLD_LOCATION)
            .setDataStoreName(COLD_LOCATION)
            .setArchive({ archiveInfo: { archiveId: `archive-${versionId || 'null'}`, archiveVersion: 1 } });
        if (versionId) {
            objmd.setVersionId(versionId);
        }
        const value = { ...objmd.getValue(), ...fields };
        // the source pipeline strips the placement
        delete value.location;
        return value;
    }

    // the entry the source pipeline produces for the document stored under
    // `documentKey`
    function replicate(documentKey, value) {
        return promisify(mqp.processKafkaEntry.bind(mqp))({
            value: JSON.stringify({ type: 'put', bucket, key: documentKey, value }),
        });
    }

    function getObject(versionId) {
        return promisify(mongoClient.getObject.bind(mongoClient))(
            bucket, KEY, versionId ? { versionId } : {}, fakeLogger);
    }

    // a null master is moved into a version document of its own once a new
    // version is put over it, and the master then follows the new version
    [
        {
            title: 'written while versioning was suspended',
            nullVersionId: VersionID.generateVersionId('', REPLICATION_GROUP_ID),
            master: versionId => coldObject(versionId, { isNull: true }),
        },
        {
            title: 'written before versioning was enabled',
            nullVersionId: VersionID.getInfVid(REPLICATION_GROUP_ID),
            master: () => coldObject(undefined),
        },
    ].forEach(({ title, nullVersionId, master }) =>
    it(`should keep a null version ${title} once a newer one replicates`, async () => {
        const newVersionId = VersionID.generateVersionId('', REPLICATION_GROUP_ID);

        const nullMaster = master(nullVersionId);
        await replicate(KEY, nullMaster);
        await replicate(`${KEY}${VID_SEP}${nullVersionId}`, {
            ...nullMaster,
            versionId: nullVersionId,
            isNull: true,
            originOp: 's3:StoreNullVersion',
        });
        const newVersion = coldObject(newVersionId, { nullVersionId });
        await replicate(`${KEY}${VID_SEP}${newVersionId}`, newVersion);
        await replicate(KEY, newVersion);

        const nullVersion = await getObject(nullVersionId);
        assert.strictEqual(nullVersion.isNull, true);
        assert.deepStrictEqual(nullVersion.archive, nullMaster.archive);
        assert.strictEqual((await getObject(newVersionId)).versionId, newVersionId);
        assert.strictEqual((await getObject()).versionId, newVersionId);
    }));

    [
        { title: 'a version', versioned: true },
        { title: 'an object with no version of its own', versioned: false },
    ].forEach(({ title, versioned }) =>
    it(`should skip a replayed entry for ${title}`, async () => {
        const versionId = versioned ?
            VersionID.generateVersionId('', REPLICATION_GROUP_ID) : undefined;
        const documentKey = versionId ? `${KEY}${VID_SEP}${versionId}` : KEY;
        // a dotted key is escaped when stored, and unescaped when read back
        const value = coldObject(versionId, { tags: { 'dotted.key': 'value' } });

        await replicate(documentKey, value);
        const putObject = sinon.spy(mongoClient, 'putObject');
        await replicate(documentKey, value);

        assert.strictEqual(putObject.callCount, 0);
    }));
});
