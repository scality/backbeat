const assert = require('assert');
const sinon = require('sinon');

const { encode } = require('arsenal').versioning.VersionID;

const ReplicationQueuePopulator =
    require('../../../extensions/replication/ReplicationQueuePopulator');
const ReplicationAPI = require('../../../extensions/replication/ReplicationAPI');

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
    obj = JSON.parse(JSON.stringify(obj));
    obj.replicationInfo.backends = backends;
    return JSON.stringify(obj);
    /* eslint-enable no-param-reassign */
}

function overwriteDataStoreName(obj, dataStoreName) {
    /* eslint-disable no-param-reassign */
    obj = JSON.parse(JSON.stringify(obj));
    obj.dataStoreName = dataStoreName;
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
            desc: 'cold object entry',
            entry: Object.assign({}, {
                type: 'put',
                bucket: 'test-bucket-source',
                key: 'a-test-key\u000098477724999464999999RG001  1.30.12',
                logReader: {
                    getMetricLabels: stubMetricLabels(),
                },
            }, { value: overwriteDataStoreName(kafkaValue, 'location-dmf-v1') }),
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

    // A "standalone null master key" is created when an object is placed in a non-versioned bucket,
    // which is then converted to a versioned bucket. If no new versioned objects are added for that object,
    // it appears as a standalone null master key with no version id.
    it('should replicate standalone null master key', () => {
        const customKafkaValue = {
            ...kafkaValue,
        };
        delete customKafkaValue.versionId;
        const entry = Object.assign({}, {
            type: 'put',
            bucket: 'test-bucket-source',
            key: '\x7FMkey0',
            logReader: {
                getMetricLabels: () => {},
            },
        }, { value: JSON.stringify(customKafkaValue) });

        rqp._filterKeyOp(entry);

        const publishedMessage = rqp.getState();
        assert(publishedMessage.key);
    });

    it('should replicate master suspended null version', () => {
        const customKafkaValue = {
            ...kafkaValue,
            versionId: '98285859405462999999RG001  ',
            isNull: true,
        };
        const entry = Object.assign({}, {
            type: 'put',
            bucket: 'test-bucket-source',
            key: '\x7FMkey0',
            logReader: {
                getMetricLabels: () => {},
            },
        }, { value: JSON.stringify(customKafkaValue) });

        rqp._filterKeyOp(entry);

        const publishedMessage = rqp.getState();
        assert(publishedMessage.key);
    });

    it('should not replicate non-null master', () => {
        const customKafkaValue = {
            ...kafkaValue,
            versionId: '98285859405462999999RG001  ',
        };
        const entry = Object.assign({}, {
            type: 'put',
            bucket: 'test-bucket-source',
            key: '\x7FMkey0',
            logReader: {
                getMetricLabels: () => {},
            },
        }, { value: JSON.stringify(customKafkaValue) });

        rqp._filterKeyOp(entry);

        const publishedMessage = rqp.getState();
        assert(!publishedMessage.key);
    });

    it('should ignore internal buckets except users..bucket', () => {
        const entry = {
            bucket: 'internal..backupIndex',
            key: 'key',
            value: '{}',
            type: 'put',
            logReader: {
                getMetricLabels: () => {},
            },
        };
        rqp.filter(entry);
        const publishedMessage = rqp.getState();
        assert.deepStrictEqual(publishedMessage, {});
    });

    it('should not ignore users..bucket', () => {
        const entry = {
            bucket: 'users..bucket',
            type: 'put',
            key: 'owner..|..bucket',
            value: '{}',
            logReader: {
                getMetricLabels: () => {},
            },
        };
        rqp.filter(entry);
        const publishedMessage = rqp.getState();
        assert(publishedMessage.key);
        assert.strictEqual(decodeURIComponent(publishedMessage.key), 'users..bucket');
    });

    // Regression test: a malformed raft entry value must not
    // crash the populator. Before this fix, `JSON.parse(entry.value)` at the
    // top of `_filterKeyOp` threw a SyntaxError on malformed input, which
    // propagated up through the synchronous filter chain and exited the
    // populator process — leading to an infinite supervisord restart loop
    // because the unchanged logOffset re-read the same bad record. The fix
    // wraps the parse with `safeJsonParse`, logs at error level with the
    // entry's identifying context, and skips the entry so the batch advances.
    it('should not throw on malformed entry value and should skip publishing', () => {
        const entry = {
            type: 'put',
            bucket: 'test-bucket-source',
            key: 'a-test-key 98477724999464999999RG001  1.30.12',
            // malformed JSON — closing brace missing mid-stream; mimics the
            // interleaved-fragment shape observed in production (RD-307).
            value: '{"owner-display-name":"x","content-length":42,"acl":{"Canned":"p"',
            logReader: {
                getMetricLabels: () => ({ logId: 'raft_test' }),
            },
        };

        assert.doesNotThrow(() => rqp._filterKeyOp(entry));
        assert.deepStrictEqual(rqp.getState(), {});
    });
});

/**
 * Records every published message, whatever the topic, so localization
 * entries (data mover topic) can be inspected.
 * @class
 */
class RecordingQueuePopulatorMock extends ReplicationQueuePopulator {
    constructor(params) {
        super(params);

        this.published = [];
    }

    publish(topic, key, message) {
        this.published.push({ topic, key, message });
    }
}

describe('replication queue populator: clean room localization', () => {
    const CRR_LOCATION = 'location-crr-source';
    const LOCAL_LOCATION = 'us-east-1';
    const RESULTS_TOPIC = 'test-transition-results';
    const VERSION_ID = '98477724999464999999RG001  1.30.12';
    const VERSIONED_KEY = `a-test-key\u0000${VERSION_ID}`;

    let params;
    let rqp;

    function makeValue(overrides = {}) {
        return JSON.stringify({
            ...kafkaValue,
            dataStoreName: CRR_LOCATION,
            location: [{
                key: 'some-data-key',
                size: 128,
                start: 0,
                dataStoreName: CRR_LOCATION,
                dataStoreETag: '1:d41d8cd98f00b204e9800118ecf8427e',
            }],
            ...overrides,
        });
    }

    function makeEntry(value, key = VERSIONED_KEY) {
        return {
            type: 'put',
            bucket: 'test-bucket-source',
            key,
            value,
            overheadFields: { commitTimestamp: '2024-05-06T10:11:12.000Z' },
            logReader: { getMetricLabels: stubMetricLabels() },
        };
    }

    beforeEach(() => {
        params = {
            config: {
                topic: TOPIC,
                localization: {
                    toLocation: LOCAL_LOCATION,
                    resultsTopic: RESULTS_TOPIC,
                },
            },
            logger: fakeLogger,
            metricsHandler: {
                bytes: sinon.spy(),
                objects: sinon.spy(),
                localizationBytes: sinon.spy(),
                localizationObjects: sinon.spy(),
            },
        };
        rqp = new RecordingQueuePopulatorMock(params);
    });

    it('should publish a copyLocation action for a non-localized object', () => {
        rqp._filterKeyOp(makeEntry(makeValue()));

        assert.strictEqual(rqp.published.length, 1);
        const [{ topic, key, message }] = rqp.published;
        assert.strictEqual(topic, ReplicationAPI.getDataMoverTopic());
        assert.strictEqual(key, 'test-bucket-source/a-test-key');

        const action = JSON.parse(message);
        assert.strictEqual(action.action, 'copyLocation');
        assert.strictEqual(action.toLocation, LOCAL_LOCATION);
        assert.strictEqual(action.resultsTopic, RESULTS_TOPIC);
        assert.strictEqual(action.contextInfo.ruleType, 'transition');
        assert.strictEqual(action.contextInfo.origin, 'localization');
        assert.deepStrictEqual(action.target, {
            owner: kafkaValue['owner-id'],
            bucket: 'test-bucket-source',
            key: 'a-test-key',
            version: encode(VERSION_ID),
            eTag: `"${kafkaValue['content-md5']}"`,
            lastModified: kafkaValue['last-modified'],
        });
        // resolved by the transition processor, not by the populator
        assert.strictEqual(action.target.accountId, undefined);
        assert.deepStrictEqual(action.source, {
            bucket: 'test-bucket-source',
            objectKey: 'a-test-key',
            storageClass: CRR_LOCATION,
        });
        assert.strictEqual(action.metrics.fromLocation, CRR_LOCATION);
        assert.strictEqual(action.metrics.contentLength, 128);
        assert.strictEqual(action.metrics.transitionTime,
            '2024-05-06T10:11:12.000Z');
    });

    it('should account localized objects and bytes', () => {
        rqp._filterKeyOp(makeEntry(makeValue()));

        sinon.assert.calledOnceWithExactly(
            params.metricsHandler.localizationBytes, labels, 128);
        sinon.assert.calledOnceWithExactly(
            params.metricsHandler.localizationObjects, labels);
        sinon.assert.notCalled(params.metricsHandler.objects);
    });

    // localization is about where the data lives, forward replication is
    // about where it has been copied to: the two are independent.
    ['PENDING', 'COMPLETED', 'FAILED'].forEach(status => {
        it(`should publish regardless of replication status ${status}`, () => {
            const value = makeValue({
                replicationInfo: { ...repInfo, status },
            });
            rqp._filterKeyOp(makeEntry(value));

            assert.strictEqual(rqp.published.length, 1);
        });
    });

    it('should publish when there is no replication configured', () => {
        const value = makeValue({ replicationInfo: null });
        rqp._filterKeyOp(makeEntry(value));

        assert.strictEqual(rqp.published.length, 1);
    });

    it('should propagate the transition attempt count', () => {
        const value = makeValue({
            'x-amz-meta-scal-s3-transition-attempt': '3',
        });
        rqp._filterKeyOp(makeEntry(value));

        assert.strictEqual(rqp.published.length, 1);
        const action = JSON.parse(rqp.published[0].message);
        assert.strictEqual(action.target.attempt, 3);
    });

    it('should not set an attempt count for a first copy', () => {
        rqp._filterKeyOp(makeEntry(makeValue()));

        const action = JSON.parse(rqp.published[0].message);
        assert.strictEqual(action.target.attempt, undefined);
    });

    it('should skip master keys', () => {
        rqp._filterKeyOp(makeEntry(makeValue(), 'a-test-key'));

        assert.strictEqual(rqp.published.length, 0);
    });

    it('should skip delete markers', () => {
        const value = makeValue({ isDeleteMarker: true });
        rqp._filterKeyOp(makeEntry(value));

        assert.strictEqual(rqp.published.length, 0);
    });

    it('should skip empty objects', () => {
        const value = makeValue({
            'location': null,
            'content-length': 0,
        });
        rqp._filterKeyOp(makeEntry(value));

        assert.strictEqual(rqp.published.length, 0);
    });

    it('should skip and report non-empty objects without location', () => {
        const errorSpy = sinon.spy(rqp.log, 'error');
        const value = makeValue({ location: null });
        rqp._filterKeyOp(makeEntry(value));

        assert.strictEqual(rqp.published.length, 0);
        sinon.assert.calledOnce(errorSpy);
        errorSpy.restore();
    });

    // partial oplog projections (change stream `update` events) may not carry
    // the location: they cannot be localized, and behave as before.
    it('should not localize entries with no dataStoreName', () => {
        const value = makeValue({ dataStoreName: undefined });
        rqp._filterKeyOp(makeEntry(value));

        sinon.assert.notCalled(params.metricsHandler.localizationObjects);
        assert.strictEqual(
            rqp.published.filter(
                p => p.topic === ReplicationAPI.getDataMoverTopic()).length,
            0);
    });

    it('should not localize objects on a regular location', () => {
        const value = makeValue({ dataStoreName: LOCAL_LOCATION });
        rqp._filterKeyOp(makeEntry(value));

        assert.strictEqual(rqp.published.length, 1);
        assert.strictEqual(rqp.published[0].topic, TOPIC);
        sinon.assert.notCalled(params.metricsHandler.localizationObjects);
    });

    it('should fall back to replication when localization is disabled', () => {
        delete params.config.localization;
        rqp = new RecordingQueuePopulatorMock(params);
        rqp._filterKeyOp(makeEntry(makeValue()));

        assert.strictEqual(rqp.published.length, 1);
        assert.strictEqual(rqp.published[0].topic, TOPIC);
    });
});
