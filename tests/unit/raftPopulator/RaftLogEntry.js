const assert = require('assert');
const BucketInfo = require('arsenal').models.BucketInfo
const RaftLogEntry = require('../../../extensions/utils/RaftLogEntry');
const constants = require('../../../constants');


const dummyObject = {

}

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

const dummyBucketMdObj = new BucketInfo(bucketMd._name, bucketMd._owner, bucketMd._ownerDisplayName,
bucketMd._creationDate, bucketMd._mdBucketModelVersion, bucketMd._acl, bucketMd._transient,
bucketMd._deleted, bucketMd._serverSideEncryption, bucketMd.versioningConfiguration,
bucketMd._);

describe('RaftLogEntry methods', () => {
    before(() => {
        this.createEntry = new RaftLogEntry();
    });

    it('should properly format an entry for object metadata', () => {

    });

    it('should properly format an entry for bucket', () => {

    });

    it('should properly format an entry for bucket metadata', () => {

    });
});
