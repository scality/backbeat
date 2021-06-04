const assert = require('assert');
const sinon = require('sinon');

const ReplicationQueuePopulator =
    require('../../../extensions/replication/ReplicationQueuePopulator');

const fakeLogger = require('../../utils/fakeLogger');

const TOPIC = 'test-topic';
const SITE = 'test-site';
const SITE2 = 'test-site2';
const labels = { a: 10 }; // dummy metric labels

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
mdOnlyKafkaValue.replicationInfo = Object.assign({},
    kafkaValue.replicationInfo,
    { content: ['METADATA'] }
);

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

function overwriteBackends(obj, backends) {
    /* eslint-disable no-param-reassign */
    obj.replicationInfo.backends = backends;
    return JSON.stringify(obj);
    /* eslint-enable no-param-reassign */
}

function stubMetricLabels() {
    const metricLabelsStub = sinon.stub();
    metricLabelsStub.returns(labels);
    return metricLabelsStub;
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

    [
        {
            desc: 'object entry, not a master key',
            entry: Object.assign({}, {
                type: 'put',
                bucket: 'test-bucket-source',
                key: 'a-test-key',
                logReader: {
                    getMetricLabels: stubMetricLabels(),
                },
            }, { value: JSON.stringify(kafkaValue) }),
            results: {},
        },
        {
            desc: 'object entry, master key',
            entry: Object.assign({}, {
                type: 'put',
                bucket: 'test-bucket-source',
                key: 'a-test-key\u000098477724999464999999RG001  1.30.12',
                logReader: {
                    getMetricLabels: stubMetricLabels(),
                },
            }, { value: JSON.stringify(kafkaValue) }),
            results: { [SITE]: { ops: 1, bytes: 128 } },
        },
        {
            desc: 'object entry, master key, multiple backend',
            entry: Object.assign({}, {
                type: 'put',
                bucket: 'test-bucket-source',
                key: 'a-test-key2\u000098477724999464999999RG001  1.30.12',
                logReader: {
                    getMetricLabels: stubMetricLabels(),
                },
            }, { value:
                overwriteBackends(kafkaValue, [
                    { site: SITE, status: 'PENDING' },
                    { site: SITE2, status: 'PENDING' },
                ]),
            }),
            results: {
                [SITE]: { ops: 1, bytes: 128 },
                [SITE2]: { ops: 1, bytes: 128 },
            },
        },
        {
            desc: 'metadata only entry, master key',
            entry: Object.assign({}, {
                type: 'put',
                bucket: 'test-bucket-source',
                key: 'a-test-key2\u000098477724999464999999RG001  1.30.12',
                logReader: {
                    getMetricLabels: stubMetricLabels(),
                },
            }, { value: JSON.stringify(mdOnlyKafkaValue) }),
            results: { [SITE]: { ops: 1, bytes: 0 } },
        },
    ].forEach(input => {
        it(`should filter entries properly: ${input.desc}`, () => {
            rqp.filter(input.entry);

            const metrics = rqp.getAndResetMetrics();

            assert.deepStrictEqual(input.results, metrics);
            const expected = Object.assign({}, input.entry);
            delete expected.logReader;

            if (Object.keys(input.results).length) {
                assert.deepStrictEqual(JSON.stringify(expected),
                    rqp.getState().message);
            } else {
                assert.deepStrictEqual(rqp.getState(), {});
            }
        });
    });

    it('publish prom metrics', () => {
        const entry = Object.assign({}, {
            type: 'put',
            bucket: 'test-bucket-source',
            key: 'a-test-key\u000098477724999464999999RG001  1.30.12',
            logReader: {
                getMetricLabels: stubMetricLabels(),
            },
        }, { value: JSON.stringify(kafkaValue) });

        rqp._filterKeyOp(entry);

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

    it('can publish when filtering bucket op', () => {
        const labels = { a: 10 }; // dummy metric labels
        const metricLabelsStub = sinon.stub();
        metricLabelsStub.returns(labels);
        const entry = Object.assign({}, {
            type: 'put',
            bucket: 'test-bucket-source',
            key: 'a-test-key\u000098477724999464999999RG001  1.30.12',
            logReader: {},
        }, { value: JSON.stringify(kafkaValue) });
        // force the circular reference
        entry.logReader.entry = entry;

        // should not throw
        rqp._filterBucketOp(entry);
    });
});
