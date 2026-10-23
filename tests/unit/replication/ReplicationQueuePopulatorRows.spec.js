const assert = require('assert');
const sinon = require('sinon');

const ReplicationQueuePopulator =
    require('../../../extensions/replication/ReplicationQueuePopulator');
const fakeLogger = require('../../utils/fakeLogger');
const { listEntries, VID } = require('../../utils/nullVersionRows');

const repInfo = {
    status: 'PENDING',
    backends: [{ site: 'test-site', status: 'PENDING', dataStoreVersionId: '' }],
    content: ['DATA', 'METADATA'],
    destination: 'arn:aws:s3:::test-bucket-target',
    storageClass: 'awsbackend',
    role: 'arn:aws:iam::922268666771:role/bb-replication-1522257577471',
    storageType: 'aws_s3',
    dataStoreVersionId: '',
};

const baseValue = {
    'owner-display-name': 'test',
    'owner-id': 'e166a2080a0c2cf1474dce54654f3f224dd5ae01379f20f338d106b8bc964bb1',
    'content-length': 128,
    'content-md5': 'd41d8cd98f00b204e9800118ecf8427e',
    'key': '',
    'location': null,
    'isDeleteMarker': false,
    'dataStoreName': 'dc-1',
    'last-modified': '2018-03-28T22:10:00.534Z',
    'md-model-version': 3,
    'replicationInfo': repInfo,
};

// master versus version key entries of each layout
describe('ReplicationQueuePopulator: master versus version entries', () => {
    let rqp;
    let batch;

    const filter = (key, value) => {
        rqp._filterKeyOp({
            type: 'put',
            bucket: 'test-bucket-source',
            key,
            value: JSON.stringify(value),
            logReader: { getMetricLabels: () => ({}) },
        });
        return Object.keys(batch).length > 0;
    };

    beforeEach(() => {
        rqp = new ReplicationQueuePopulator({
            config: { topic: 'test-topic' },
            logger: fakeLogger,
            metricsHandler: { bytes: sinon.spy(), objects: sinon.spy() },
        });
        batch = {};
        rqp.setBatch(batch);
    });

    listEntries(baseValue).forEach(({ desc, key, value, shadowed }) => {
        it(`should ${shadowed ? 'skip' : 'replicate'} ${desc}`, () => {
            assert.strictEqual(filter(key, value), !shadowed);
        });
    });

    it('should replicate the master of a versioned object from an NFS bucket', () => {
        const value = {
            ...baseValue,
            versionId: VID,
            replicationInfo: { ...repInfo, isNFS: true },
        };
        assert.strictEqual(filter('obj', value), true);
    });
});
