const assert = require('assert');
const BucketInfo = require('arsenal').models.BucketInfo;
const RaftLogEntry = require('../../../extensions/utils/RaftLogEntry');
const constants = require('../../../constants');

const dummyObject = {
    res: {
        'owner-display-name': 'test_1518720219',
        'owner-id':
            '94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8',
        'content-length': 0,
        'content-md5': 'd41d8cd98f00b204e9800998ecf8427e',
        'x-amz-version-id': 'null',
        'x-amz-server-version-id': '',
        'x-amz-storage-class': 'STANDARD',
        'x-amz-server-side-encryption': '',
        'x-amz-server-side-encryption-aws-kms-key-id': '',
        'x-amz-server-side-encryption-customer-algorithm': '',
        'x-amz-website-redirect-location': '',
        'acl': {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        },
        'key': '',
        'location': null,
        'isDeleteMarker': false,
        'tags': {},
        'replicationInfo': {
            status: '',
            backends: [],
            content: [],
            destination: '',
            storageClass: '',
            role: '',
            storageType: '',
            dataStoreVersionId: '',
        },
        'dataStoreName': 'us-east-1',
        'last-modified': '2018-02-16T22:43:37.174Z',
        'md-model-version': 3,
    },
    objectKey: 'afternoon',
    bucketName: 'xxxnewbucket',
};

const dummyBucket = {
    key: '94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8' +
        '..|..xxxnewbucket',
    value: {
        Size: undefined,
        ETag: undefined,
        VersionId: undefined,
        IsNull: undefined,
        IsDeleteMarker: undefined,
        LastModified: undefined,
        Owner: { DisplayName: undefined, ID: undefined },
        StorageClass: undefined,
        Initiated: undefined,
        Initiator: undefined,
        EventualStorageBucket: undefined,
        partLocations: undefined,
        creationDate: '2018-02-16T22:41:00.687Z',
    },
};

const bucketMd = {
    _acl: {
        Canned: 'private',
        FULL_CONTROL: [],
        WRITE: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [] },
    _name: 'xxxfriday10',
    _owner: '94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8',
    _ownerDisplayName: 'test_1518720219',
    _creationDate: '2018-02-16T21:55:16.415Z',
    _mdBucketModelVersion: 5,
    _transient: false,
    _deleted: false,
    _serverSideEncryption: null,
    _versioningConfiguration: null,
    _locationConstraint: 'us-east-1',
    _websiteConfiguration: null,
    _replicationConfiguration: null,
    _cors: null,
    _lifecycleConfiguration: null,
    _uid: undefined,
};

const dummyBucketMdObj = new BucketInfo(bucketMd._name, bucketMd._owner,
    bucketMd._ownerDisplayName, bucketMd._creationDate,
    bucketMd._mdBucketModelVersion, bucketMd._acl, bucketMd._transient,
    bucketMd._deleted, bucketMd._serverSideEncryption,
    bucketMd.versioningConfiguration, bucketMd._locationContraint,
    bucketMd._websiteConfiguration, bucketMd._cors, bucketMd._lifeCycle);

describe('RaftLogEntry methods', () => {
    before(() => {
        this.createEntry = new RaftLogEntry();
    });

    it('should properly format an entry for object metadata', () => {
        const objectMdEntry = this.createEntry.createPutEntry(dummyObject);
        assert(objectMdEntry);
        assert.strictEqual(objectMdEntry.type, 'put');
        assert.strictEqual(objectMdEntry.bucket, dummyObject.bucketName);
        assert.strictEqual(objectMdEntry.key, dummyObject.objectKey);
        assert.strictEqual(typeof objectMdEntry.value, 'string');
        assert.strictEqual(objectMdEntry.value,
            JSON.stringify(dummyObject.res));
    });

    it('should properly format an entry for bucket', () => {
        const bucketEntry = this.createEntry.createPutBucketEntry(dummyBucket);
        assert(bucketEntry);
        assert.strictEqual(bucketEntry.type, 'put');
        assert.strictEqual(bucketEntry.bucket, constants.usersBucket);
        assert.strictEqual(bucketEntry.key, dummyBucket.key);
        assert.strictEqual(typeof bucketEntry.value, 'string');
        assert.strictEqual(bucketEntry.value,
            JSON.stringify(dummyBucket.value));
    });

    it('should properly format an entry for bucket metadata', () => {
        const bucketMdEntry =
            this.createEntry.createPutBucketMdEntry(dummyBucketMdObj);
        assert(bucketMdEntry);
        assert.strictEqual(bucketMdEntry.type, 'put');
        assert.strictEqual(bucketMdEntry.bucket, dummyBucketMdObj._name);
        assert.strictEqual(bucketMdEntry.key, dummyBucketMdObj._name);
        assert.strictEqual(typeof bucketMdEntry.value, 'string');
        assert.strictEqual(bucketMdEntry.value, dummyBucketMdObj.serialize());
    });
});
