const assert = require('assert');
const sinon = require('sinon');

const ReplicationQueuePopulator =
    require('../../../extensions/replication/ReplicationQueuePopulator');

const fakeLogger = require('../../utils/fakeLogger');

const TOPIC = 'test-topic';
const SITE = 'test-site';

const repInfo = {
    status: 'PENDING',
    backends: [{
        site: SITE,
        status: 'PENDING',
        dataStoreVersionId: '',
    }],
    content: ['DATA', 'METADATA'],
    destination: 'arn:aws:s3:::test-bucket-target',
    storageClass: 'awsbackend',
    role: 'arn:aws:iam::922268666771:role/bb-replication-1522257577471',
    storageType: 'aws_s3',
    dataStoreVersionId: '',
};

const kafkaValue = {
    'owner-display-name': 'test_1522198049',
    'owner-id': 'e166a2080a0c2cf1474dce54654f3f224dd5ae01379f20f338d106b8bc964bb1',
    'content-length': 128,
    'content-md5': 'd41d8cd98f00b204e9800118ecf8427e',
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
    'dataStoreName': 'dc-1',
    'last-modified': '2018-03-28T22:10:00.534Z',
    'md-model-version': 3,
    'versionId': '98477724999464999999RG001  1.30.12',
    'replicationInfo': repInfo,
};

const mdOnlyKafkaValue = Object.assign({}, kafkaValue);
mdOnlyKafkaValue.replicationInfo.content = ['METADATA'];

/**
 * This mock object is to overwrite the `publish` method and add a way of
 * getting information on published messages.
 * @class
 */
class ReplicationQueuePopulatorMock extends ReplicationQueuePopulator {
    constructor(params) {
        super(params);

        this._state = {};
    }

    publish(topic, key, message) {
        assert.strictEqual(topic, TOPIC);

        this._state.key = encodeURIComponent(key);
        this._state.message = message;
    }

    getState() {
        return this._state;
    }

    resetState() {
        this._state = {};
    }
}

describe('replication queue populator', () => {
    let params;
    let rqp;

    beforeEach(() => {
        params = {
            config: {
                topic: TOPIC,
            },
            logger: fakeLogger,
            metricsHandler: {
                bytes: sinon.spy(),
                objects: sinon.spy(),
            },
        };
        rqp = new ReplicationQueuePopulatorMock(params);
    });

    afterEach(() => {
        rqp.resetState();
    });

    it('publish prom metrics', () => {
        const labels = { a: 10 }; // dummy metric labels
        const metricLabelsStub = sinon.stub();
        metricLabelsStub.returns(labels);
        const entry = Object.assign({}, {
            type: 'put',
            bucket: 'test-bucket-source',
            key: 'a-test-key\u000098477724999464999999RG001  1.30.12',
            logReader: {
                getMetricLabels: metricLabelsStub,
            },
        }, { value: JSON.stringify(kafkaValue) });

        rqp._filterVersionedKey(entry);

        sinon.assert.calledOnceWithExactly(
            params.metricsHandler.bytes,
            labels,
            128
        );
        sinon.assert.calledOnceWithExactly(
            params.metricsHandler.objects,
            labels
        );
    });
});
