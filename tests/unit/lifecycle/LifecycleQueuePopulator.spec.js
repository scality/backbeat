const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');
const { encode } = require('arsenal').versioning.VersionID;

const config = require('../../../lib/Config');
const {
    coldStorageRestoreAdjustTopicPrefix,
    coldStorageRestoreTopicPrefix,
    coldStorageGCTopicPrefix,
    coldStorageArchiveTopicPrefix
} = config.extensions.lifecycle;

const LifecycleQueuePopulator = require('../../../extensions/lifecycle/LifecycleQueuePopulator');
const { errors } = require('arsenal');

const logger = new werelogs.Logger('test:LifecycleQueuePopulator');

const params = {
    authConfig: {
        transport: 'http',
    },
    logger,
};
const coldLocationConfigs = {
    'dmf-v1': {
        isCold: true,
        type: 'tlp',
    },
    'dmf-v2': {
        isCold: true,
        type: 'tlp',
    },
};
const locationConfigs = {
    'us-east-1': {
        type: 'aws_s3',
    },
    'us-east-2': {
        type: 'aws_s3',
    },
};

const templateEntry = {
    'md-model-version': 2,
    'owner-display-name': 'Bart',
    'owner-id': '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
    'content-length': 542,
    'content-type': 'text/plain',
    'last-modified': '2017-07-13T02:44:25.519Z',
    'content-md5': '01064f35c238bd2b785e34508c3d27f4',
    'x-amz-version-id': 'null',
    'x-amz-server-version-id': '',
    'x-amz-storage-class': 'dmf-v1',
    'x-amz-server-side-encryption': '',
    'x-amz-server-side-encryption-aws-kms-key-id': '',
    'x-amz-server-side-encryption-customer-algorithm': '',
    'x-amz-website-redirect-location': '',
    'acl': {
      Canned: 'private',
      FULL_CONTROL: [],
      WRITE_ACP: [],
      READ: [],
      READ_ACP: []
    },
    'key': 'hosts',
    'location': [
      {
        key: '29258f299ddfd65f6108e6cd7bd2aea9fbe7e9e0',
        size: 542,
        start: 0,
        dataStoreName: 'file',
        dataStoreETag: '1:01064f35c238bd2b785e34508c3d27f4'
      }
    ],
    'isDeleteMarker': false,
    'tags': {},
    'replicationInfo': {},
    'versionId': '98500086134471999999RG001  0',
    'isNFS': true,
    'archive': {
        archiveInfo: {
            archiveId: '04425717-a65c-4e8a-95e1-fa1d902d9d9f',
            archiveVersion: 7504504064263669,
        },
        restoreRequestedAt: Date.now(),
        restoreRequestedDays: 1,
    },
    'dataStoreName': 'us-east-1',
};

function getKafkaEntry(originOp) {
    const entry = templateEntry;
    entry.originOp = originOp;
    return {
        type: 'put',
        bucket: 'lc-queue-populator-test-bucket',
        key: 'hosts\x0098500086134471999999RG001  0',
        value: JSON.stringify(entry),
    };
}

describe('LifecycleQueuePopulator', () => {
    function _stubSetupProducer(topic, cb) {
        // fake producer connection
        setTimeout(() => {
            this._producers[topic] = {
                send: () => {},
            };
            return cb();
        }, 100);
    }

    describe('constructor', () => {
        it('should not create vaultClientWrapper when no auth config passed', () => {
            const params = {};
            const lcqp = new LifecycleQueuePopulator(params);
            assert.strictEqual(lcqp.vaultClientWrapper, undefined);
        });
    });

    describe('Producer', () => {
        let lcqp;
        beforeEach(() => {
            lcqp = new LifecycleQueuePopulator(params);
            sinon.stub(lcqp, '_setupProducer').callsFake(_stubSetupProducer);
        });
        afterEach(() => {
            sinon.restore();
        });
        it('should not setup producers if no cold locations are configured', done => {
            lcqp.locationConfigs = locationConfigs;
            lcqp.setupProducers(() => {
                const producers = Object.keys(lcqp._producers);
                assert.strictEqual(producers.length, 0);
                done();
            });
        });
        it('should have four producers per cold location', done => {
            lcqp.locationConfigs = Object.assign({}, locationConfigs, coldLocationConfigs);
            lcqp.setupProducers(() => {
                const producers = Object.keys(lcqp._producers);
                const coldLocations = Object.keys(coldLocationConfigs);
                assert.strictEqual(producers.length, coldLocations.length * 4);
                coldLocations.forEach(loc => {
                    assert(producers.includes(`${coldStorageRestoreAdjustTopicPrefix}${loc}`));
                    assert(producers.includes(`${coldStorageRestoreTopicPrefix}${loc}`));
                    assert(producers.includes(`${coldStorageGCTopicPrefix}${loc}`));
                    assert(producers.includes(`${coldStorageArchiveTopicPrefix}${loc}`));
                });
                done();
            });
        });
    });

    describe(':_handleRestoreOp', () => {
        let lcqp;
        const getAccountIdStub = sinon.stub().yields(null,
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be');
        beforeEach(() => {
            lcqp = new LifecycleQueuePopulator(params);
            lcqp.locationConfigs = Object.assign({}, coldLocationConfigs, locationConfigs);
            lcqp.vaultClientWrapper = {
                getAccountId: getAccountIdStub,
            };
        });
        afterEach(() => {
            sinon.restore();
        });
        [
            {
                event: 's3:ObjectRestore',
                ignore: false,
            },
            {
                event: 's3:ObjectRestore:Post',
                ignore: false,
            },
            {
                event: 's3:ObjectRestore:Retry',
                ignore: false,
            },
            {
                event: 's3:ObjectCreated:Put',
                ignore: true,
            },
        ].forEach(params => {
            const outcome = params.ignore ? 'ignore' : 'consider';
            it(`should ${outcome} ${params.event} event`, () => {
                const getAccountIdStub = sinon.stub().yields(null,
                    '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be');
                        lcqp.vaultClientWrapper = {
                    getAccountId: getAccountIdStub,
                };
                const entry = getKafkaEntry(params.event);
                lcqp._handleRestoreOp(entry);
                assert.strictEqual(getAccountIdStub.calledOnce, !params.ignore);
            });
        });

        describe('restore requests', () => {
            const kafkaSendStub = sinon.stub().yields();
            const kafkaAdjustSendStub = sinon.stub().yields();
            let clock;

            beforeEach(() => {
                clock = sinon.useFakeTimers({
                    now: 1499913865515,
                });
                lcqp._producers[`${coldStorageRestoreAdjustTopicPrefix}dmf-v1`] = {
                    send: kafkaAdjustSendStub,
                };
                lcqp._producers[`${coldStorageRestoreTopicPrefix}dmf-v1`] = {
                    send: kafkaSendStub,
                };
            });

            afterEach(() => {
                kafkaSendStub.reset();
                kafkaAdjustSendStub.reset();
                clock.restore();
            });

            [
                {
                    requestDays: 3,
                    timeProgressionFactor: 1,
                    sentDurationSecs: 259200,
                },
                {
                    requestDays: 3,
                    timeProgressionFactor: 17281, // 3 days in 15 seconds
                    sentDurationSecs: 15,
                },
            ].forEach(params => {
                const p = params.timeProgressionFactor;
                it(`should send restore duration in initial restore request, with time factor ${p}`, () => {
                    config.timeOptions.timeProgressionFactor = params.timeProgressionFactor;

                    const objMd = {
                        'md-model-version': 2,
                        'owner-display-name': 'Bart',
                        'owner-id': '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                        'x-amz-storage-class': 'dmf-v1',
                        'content-length': 542,
                        'content-type': 'text/plain',
                        'last-modified': '2017-07-13T02:44:25.515Z',
                        'content-md5': '01064f35c238bd2b785e34508c3d27f4',
                        'key': 'object',
                        'location': [],
                        'isDeleteMarker': false,
                        'isNull': false,
                        'archive': {
                            archiveInfo: {
                                archiveId: '04425717-a65c-4e8a-95e1-fa1d902d9d9f',
                                archiveVersion: 7504504064263669
                            },
                            restoreRequestedAt: '2017-07-11T02:44:25.515Z',
                            restoreRequestedDays: params.requestDays,
                        },
                        'dataStoreName': 'dmf-v1',
                        'originOp': 's3:ObjectRestore:Post',
                    };
                    const entry = {
                        type: 'put',
                        bucket: 'lc-queue-populator-test-bucket',
                        key: 'object',
                        value: JSON.stringify(objMd),
                    };

                    lcqp._handleRestoreOp(entry);

                    assert(!kafkaAdjustSendStub.calledOnce);
                    assert(kafkaSendStub.calledOnce);

                    const message = JSON.parse(kafkaSendStub.args[0][0][0].message);
                    const expectedMessage = {
                        accountId: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                        bucketName: 'lc-queue-populator-test-bucket',
                        objectKey: 'object',
                        eTag: '01064f35c238bd2b785e34508c3d27f4',
                        archiveInfo: {
                            archiveId: '04425717-a65c-4e8a-95e1-fa1d902d9d9f',
                            archiveVersion: 7504504064263669
                        },
                        requestedDurationSecs: params.sentDurationSecs,
                        requestId: message.requestId,
                        transitionTime: '2017-07-11T02:44:25.515Z',
                    };
                    assert.deepStrictEqual(message, expectedMessage);
                });
            });

            it('should send duration-adjust message for already restored objects', () => {
                const objMd = {
                    'md-model-version': 2,
                    'owner-display-name': 'Bart',
                    'owner-id': '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                    'x-amz-storage-class': 'dmf-v1',
                    'content-length': 542,
                    'content-type': 'text/plain',
                    'last-modified': '2017-07-13T02:44:25.515Z',
                    'content-md5': '01064f35c238bd2b785e34508c3d27f4',
                    'key': 'object',
                    'location': [],
                    'isDeleteMarker': false,
                    'isNull': false,
                    'archive': {
                        archiveInfo: {
                            archiveId: '04425717-a65c-4e8a-95e1-fa1d902d9d9f',
                            archiveVersion: 7504504064263669
                        },
                        restoreCompletedAt: '2017-07-13T02:44:25.519Z',
                        restoreWillExpireAt: '2017-07-15T02:44:25.519Z',
                    },
                    'dataStoreName': 'dmf-v1',
                    'originOp': 's3:ObjectRestore:Post',
                };
                const entry = {
                    type: 'put',
                    bucket: 'lc-queue-populator-test-bucket',
                    key: 'object',
                    value: JSON.stringify(objMd),
                };

                lcqp._handleRestoreOp(entry);

                assert(kafkaAdjustSendStub.calledOnce);
                assert(!kafkaSendStub.calledOnce);

                const message = JSON.parse(kafkaAdjustSendStub.args[0][0][0].message);
                const expectedMessage = {
                    adjust: {
                        restoreWillExpireAt: '2017-07-15T02:44:25.519Z',
                    },
                    archiveInfo: {
                        archiveId: '04425717-a65c-4e8a-95e1-fa1d902d9d9f',
                        archiveVersion: 7504504064263669
                    },
                    requestId: message.requestId,
                    updatedAt: '2017-07-13T02:44:25.515Z',
                };
                assert.deepStrictEqual(message, expectedMessage);
            });

            it('should not send duration-adjust message for already expired restored objects', () => {
                const objMd = {
                    'md-model-version': 2,
                    'owner-display-name': 'Bart',
                    'owner-id': '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                    'x-amz-storage-class': 'dmf-v1',
                    'content-length': 542,
                    'content-type': 'text/plain',
                    'last-modified': '2017-07-13T02:44:25.515Z',
                    'content-md5': '01064f35c238bd2b785e34508c3d27f4',
                    'key': 'object',
                    'location': [],
                    'isDeleteMarker': false,
                    'isNull': false,
                    'archive': {
                        archiveInfo: {
                            archiveId: '04425717-a65c-4e8a-95e1-fa1d902d9d9f',
                            archiveVersion: 7504504064263669
                        },
                        restoreCompletedAt: '2017-07-13T02:44:25.519Z',
                        restoreWillExpireAt: '2017-07-12T02:44:25.519Z',
                    },
                    'dataStoreName': 'dmf-v1',
                    'originOp': 's3:ObjectRestore:Post',
                };
                const entry = {
                    type: 'put',
                    bucket: 'lc-queue-populator-test-bucket',
                    key: 'object',
                    value: JSON.stringify(objMd),
                };

                lcqp._handleRestoreOp(entry);

                assert(!kafkaAdjustSendStub.calledOnce);
                assert(!kafkaSendStub.calledOnce);
            });
        });
    });

    describe(':_handleTransitionOp', () => {
        const accountId = '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
        const versionId = '98500086134471999999RG001  0';
        const archiveTopic = `${coldStorageArchiveTopicPrefix}dmf-v1`;

        let lcqp;
        let getAccountIdStub;
        let kafkaSendStub;

        function getTransitionEntry(overrides) {
            const value = Object.assign({
                'md-model-version': 2,
                'owner-display-name': 'Bart',
                'owner-id': accountId,
                'content-length': 542,
                'content-type': 'text/plain',
                'last-modified': '2017-07-13T02:44:25.519Z',
                'content-md5': '01064f35c238bd2b785e34508c3d27f4',
                'x-amz-storage-class': 'dmf-v1',
                'x-amz-scal-transition-in-progress': true,
                'x-amz-scal-transition-time': '2017-07-13T02:44:20.000Z',
                'key': 'hosts',
                'location': [],
                'isDeleteMarker': false,
                'isNull': false,
                versionId,
                'dataStoreName': 'us-east-1',
                'originOp': 's3:ObjectCreated:Put',
            }, overrides);
            // allow overrides to remove a field by passing `undefined`
            Object.keys(value).forEach(k => {
                if (value[k] === undefined) {
                    delete value[k];
                }
            });
            return {
                type: 'put',
                bucket: 'lc-queue-populator-test-bucket',
                key: `hosts\x00${versionId}`,
                value: JSON.stringify(value),
            };
        }

        beforeEach(() => {
            lcqp = new LifecycleQueuePopulator(params);
            lcqp.locationConfigs = Object.assign({}, coldLocationConfigs, locationConfigs);
            getAccountIdStub = sinon.stub().yields(null, accountId);
            lcqp.vaultClientWrapper = {
                getAccountId: getAccountIdStub,
            };
            kafkaSendStub = sinon.stub().yields();
            lcqp._producers[archiveTopic] = {
                send: kafkaSendStub,
            };
        });

        afterEach(() => {
            sinon.restore();
        });

        [
            { originOp: 's3:ObjectCreated:Put', ignore: false },
            { originOp: 's3:ObjectCreated:CompleteMultipartUpload', ignore: false },
            { originOp: 's3:ObjectCreated:Copy', ignore: false },
            { originOp: 's3:LifecycleTransition:Retry', ignore: false },
            { originOp: 's3:LifecycleTransition:Start', ignore: true },
            { originOp: 's3:LifecycleTransition:SetArchive', ignore: true },
            { originOp: 's3:LifecycleTransition:Direct', ignore: true },
            { originOp: 's3:LifecycleTransition', ignore: true },
            { originOp: 's3:ObjectRestore:Post', ignore: true },
        ].forEach(({ originOp, ignore }) => {
            const outcome = ignore ? 'ignore' : 'consider';
            it(`should ${outcome} ${originOp} event`, () => {
                lcqp._handleTransitionOp(getTransitionEntry({ originOp }));
                assert.strictEqual(kafkaSendStub.calledOnce, !ignore);
            });
        });

        it('should publish an archive request matching the bucket processor message', () => {
            lcqp._handleTransitionOp(getTransitionEntry());

            assert(kafkaSendStub.calledOnce);
            const kafkaEntry = kafkaSendStub.args[0][0][0];
            assert.strictEqual(kafkaEntry.key, 'lc-queue-populator-test-bucket/hosts');

            const message = JSON.parse(kafkaEntry.message);
            assert.deepStrictEqual(message, {
                accountId,
                bucketName: 'lc-queue-populator-test-bucket',
                objectKey: 'hosts',
                objectVersion: encode(versionId),
                requestId: message.requestId,
                size: 542,
                eTag: '"01064f35c238bd2b785e34508c3d27f4"',
                transitionTime: '2017-07-13T02:44:20.000Z',
            });
            assert(message.requestId);
        });

        it('should fall back on last-modified when no transition time is set', () => {
            lcqp._handleTransitionOp(getTransitionEntry({
                'x-amz-scal-transition-time': undefined,
            }));

            assert(kafkaSendStub.calledOnce);
            const message = JSON.parse(kafkaSendStub.args[0][0][0].message);
            assert.strictEqual(message.transitionTime, '2017-07-13T02:44:25.519Z');
        });

        it('should publish the transition attempt count', () => {
            lcqp._handleTransitionOp(getTransitionEntry({
                'originOp': 's3:LifecycleTransition:Retry',
                'x-amz-meta-scal-s3-transition-attempt': '3',
            }));

            assert(kafkaSendStub.calledOnce);
            const message = JSON.parse(kafkaSendStub.args[0][0][0].message);
            assert.strictEqual(message.try, 3);
        });

        it('should not set objectVersion for a non-versioned object', () => {
            const entry = getTransitionEntry({ versionId: undefined });
            entry.key = 'hosts';
            lcqp._handleTransitionOp(entry);

            assert(kafkaSendStub.calledOnce);
            const message = JSON.parse(kafkaSendStub.args[0][0][0].message);
            assert.strictEqual(message.objectVersion, undefined);
        });

        [
            {
                desc: 'transition is not in progress',
                overrides: { 'x-amz-scal-transition-in-progress': undefined },
            },
            {
                desc: 'the storage class is not cold',
                overrides: { 'x-amz-storage-class': 'us-east-2' },
            },
            {
                desc: 'the object has no storage class',
                overrides: { 'x-amz-storage-class': undefined },
            },
            {
                desc: 'the data is already in the cold location',
                overrides: { dataStoreName: 'dmf-v1' },
            },
            {
                desc: 'the object is already archived',
                overrides: {
                    archive: {
                        archiveInfo: {
                            archiveId: '04425717-a65c-4e8a-95e1-fa1d902d9d9f',
                            archiveVersion: 7504504064263669,
                        },
                    },
                },
            },
        ].forEach(({ desc, overrides }) => {
            it(`should not publish when ${desc}`, () => {
                lcqp._handleTransitionOp(getTransitionEntry(overrides));
                assert(!getAccountIdStub.called);
                assert(!kafkaSendStub.called);
            });
        });

        it('should skip the master key of a versioned object', () => {
            const entry = getTransitionEntry();
            entry.key = 'hosts';
            lcqp._handleTransitionOp(entry);
            assert(!kafkaSendStub.called);
        });

        it('should skip mpu shadow bucket entries', () => {
            const entry = getTransitionEntry();
            entry.key = `mpuShadowBucket${entry.key}`;
            lcqp._handleTransitionOp(entry);
            assert(!kafkaSendStub.called);
        });

        it('should skip delete operations', () => {
            const entry = getTransitionEntry();
            entry.type = 'delete';
            lcqp._handleTransitionOp(entry);
            assert(!kafkaSendStub.called);
        });

        it('should do nothing without a vault client', () => {
            lcqp.vaultClientWrapper = null;
            lcqp._handleTransitionOp(getTransitionEntry());
            assert(!kafkaSendStub.called);
        });

        it('should not publish when the account cannot be resolved', () => {
            getAccountIdStub.yields(errors.InternalError);
            lcqp._handleTransitionOp(getTransitionEntry());
            assert(!kafkaSendStub.called);
        });

        it('should not throw when no producer is available', () => {
            delete lcqp._producers[archiveTopic];
            lcqp._handleTransitionOp(getTransitionEntry());
            assert(getAccountIdStub.calledOnce);
        });
    });

    describe(':filter', () => {
        const bucketMD = {
            name: 'lc-queue-populator-test-bucket',
            owner: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            ownerDisplayName: 'Bart',
            creationDate: '2017-07-13T02:44:25.519Z',
            mdBucketModelVersion: 10
        };

        let lcqp;
        beforeEach(() => {
            lcqp = new LifecycleQueuePopulator({
                ...params,
                config: config.extensions.lifecycle,
            });
            lcqp.locationConfigs = Object.assign({}, coldLocationConfigs, locationConfigs);
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should call _handleDeleteOp on delete message', () => {
            const handleDeleteStub = sinon.stub(lcqp, '_handleDeleteOp').returns();
            lcqp.filter({
                type: 'delete',
                bucket: 'lc-queue-populator-test-bucket',
                key: 'hosts\x0098500086134471999999RG001  0',
                value: JSON.stringify(templateEntry),
            });
            assert(handleDeleteStub.calledOnce);
        });

        it('should call _handleTransitionOp on put message', () => {
            const handleTransitionStub = sinon.stub(lcqp, '_handleTransitionOp').returns();
            lcqp.filter(getKafkaEntry('s3:ObjectCreated:Put'));
            assert(handleTransitionStub.calledOnce);
        });

        it('should not update zookeeper when bucketSource is mongodb (default)', () => {
            lcqp.extConfig.conductor.bucketSource = 'mongodb';
            const putEntry = {
                type: 'put',
                bucket: 'lc-queue-populator-test-bucket',
                key: 'attributes',
                value: JSON.stringify(bucketMD),
            };

            const updateZkStub = sinon.stub(lcqp, '_updateZkBucketNode');
            lcqp.filter(putEntry);
            assert(!updateZkStub.called);
        });

        it('should not call _updateZkBucketNode for non-bucket entries', () => {
            lcqp.extConfig.conductor.bucketSource = 'zookeeper';
            const putEntry = {
                type: 'put',
                bucket: 'filemd',
                key: 'some/object/key',
                value: JSON.stringify(templateEntry),
            };

            const updateZkStub = sinon.stub(lcqp, '_updateZkBucketNode');
            lcqp.filter(putEntry);
            assert(!updateZkStub.called);
        });

        it('should call _updateZkBucketNode for filemd bucket entry', () => {
            lcqp.extConfig.conductor.bucketSource = 'zookeeper';
            const putEntry = {
                type: 'put',
                bucket: '__metastore',
                key: 'lc-queue-populator-test-bucket',
                value: JSON.stringify(bucketMD),
            };

            const updateZkStub = sinon.stub(lcqp, '_updateZkBucketNode');
            lcqp.filter(putEntry);
            assert(updateZkStub.calledOnce);
            assert(updateZkStub.calledWithMatch(bucketMD));
        });

        it('should not call _updateZkBucketNode for filemd shadow bucket entry', () => {
            lcqp.extConfig.conductor.bucketSource = 'zookeeper';
            const putEntry = {
                type: 'put',
                bucket: '__metastore',
                key: 'mpuShadowBucket-lc-queue-populator-test-bucket',
                value: JSON.stringify(bucketMD),
            };

            const updateZkStub = sinon.stub(lcqp, '_updateZkBucketNode');
            lcqp.filter(putEntry);
            assert(!updateZkStub.called);
        });

        it('should handle error parsing filemd bucket entry', () => {
            lcqp.extConfig.conductor.bucketSource = 'zookeeper';
            const putEntry = {
                type: 'put',
                bucket: '__metastore',
                key: 'lc-queue-populator-test-bucket',
                value: 'foo{}',
            };

            const updateZkStub = sinon.stub(lcqp, '_updateZkBucketNode');
            lcqp.filter(putEntry);
            assert(!updateZkStub.calledOnce);
        });

        it('should call _updateZkBucketNode for bucketd bucket entry', () => {
            lcqp.extConfig.conductor.bucketSource = 'zookeeper';
            const putEntry = {
                type: 'put',
                bucket: 'lc-queue-populator-test-bucket',
                value: JSON.stringify({
                    attributes: JSON.stringify(bucketMD),
                }),
            };

            const updateZkStub = sinon.stub(lcqp, '_updateZkBucketNode');
            lcqp.filter(putEntry);
            assert(updateZkStub.calledOnce);
            assert(updateZkStub.calledWithMatch(bucketMD));
        });

        it('should handle error parsing bucketd bucket entry', () => {
            lcqp.extConfig.conductor.bucketSource = 'zookeeper';
            const putEntry = {
                type: 'put',
                bucket: 'lc-queue-populator-test-bucket',
                value: 'foo{}',
            };

            const updateZkStub = sinon.stub(lcqp, '_updateZkBucketNode');
            lcqp.filter(putEntry);
            assert(!updateZkStub.calledOnce);
        });

        it('should handle bucketd bucket entry without attributes', () => {
            lcqp.extConfig.conductor.bucketSource = 'zookeeper';
            const putEntry = {
                type: 'put',
                bucket: 'lc-queue-populator-test-bucket',
                value: JSON.stringify({}),
            };

            const updateZkStub = sinon.stub(lcqp, '_updateZkBucketNode');
            lcqp.filter(putEntry);
            assert(!updateZkStub.calledOnce);
        });

        it('should not call _updateZkBucketNode for delete operations', () => {
            lcqp.extConfig.conductor.bucketSource = 'zookeeper';

            const updateZkStub = sinon.stub(lcqp, '_updateZkBucketNode').yields();
            const deleteEntry = {
                type: 'delete',
                bucket: '__metastore',
                key: 'lc-queue-populator-test-bucket',
            };
            lcqp.filter(deleteEntry);
            assert(!updateZkStub.called);
        });
    });

    describe(':_handleDeleteOp', () => {
        const kafkaSendStub = sinon.stub().yields();
        const objMd = {
            'md-model-version': 2,
            'owner-display-name': 'Bart',
            'owner-id': '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            'x-amz-storage-class': 'dmf-v1',
            'content-length': 542,
            'content-type': 'text/plain',
            'last-modified': '2017-07-13T02:44:25.519Z',
            'content-md5': '01064f35c238bd2b785e34508c3d27f4',
            'key': 'object',
            'location': [],
            'isDeleteMarker': false,
            'isNull': false,
            'archive': {
                archiveInfo: {
                    archiveId: '04425717-a65c-4e8a-95e1-fa1d902d9d9f',
                    archiveVersion: 7504504064263669
                },
            },
            'dataStoreName': 'dmf-v1',
        };
        let lcqp;
        beforeEach(() => {
            lcqp = new LifecycleQueuePopulator(params);
            lcqp.locationConfigs = Object.assign({}, coldLocationConfigs, locationConfigs);
            lcqp._producers[`${coldStorageGCTopicPrefix}us-east-1`] = {
                send: kafkaSendStub,
            };
            lcqp._producers[`${coldStorageGCTopicPrefix}dmf-v1`] = {
                send: kafkaSendStub,
            };
        });
        afterEach(() => {
            kafkaSendStub.reset();
        });
        [
            {
                it: 'should skip non dmf archived/restored objects',
                type: 'delete',
                key: 'object',
                md: {
                    ...objMd,
                    'archive': {},
                    'x-amz-storage-class': 'STANDARD',
                    'dataStoreName': 'us-east-1',
                },
                getAccountIdResponse: [null, '1234'],
                called: false,
            },
            {
                it: 'should skip versioned masters',
                type: 'delete',
                key: 'object',
                md: {
                    ...objMd,
                    versionId: '98500086134471999999RG001  0',
                },
                getAccountIdResponse: [null, '1234'],
                called: false,
            },
            {
                it: 'should skip null versioned version',
                type: 'delete',
                key: 'object\x0098500086134471999999RG001  0',
                md: {
                    ...objMd,
                    versionId: '98500086134471999999RG001  0',
                    isNull: true,
                },
                getAccountIdResponse: [null, '1234'],
                called: false,
            },
            {
                it: 'should skip delete marker',
                type: 'delete',
                key: 'object',
                md: {
                    ...objMd,
                    isDeleteMarker: true,
                },
                getAccountIdResponse: [null, '1234'],
                called: false,
            },
            {
                it: 'should skip if location config is not found',
                type: 'delete',
                key: 'object',
                md: {
                    ...objMd,
                    'archive': {},
                    'x-amz-storage-class': 'azure-archive',
                    'dataStoreName': 'azure-archive',
                },
                getAccountIdResponse: [null, '1234'],
                called: false,
            },
            {
                it: 'should process version',
                type: 'delete',
                key: 'object\x0098500086134471999999RG001  0',
                md: {
                    ...objMd,
                    versionId: '98500086134471999999RG001  0',
                },
                getAccountIdResponse: [null, '1234'],
                called: true,
            },
            {
                it: 'should process non versioned master',
                type: 'delete',
                key: 'object',
                md: {
                    ...objMd,
                },
                getAccountIdResponse: [null, '1234'],
                called: true,
            },
            {
                it: 'should process null versioned master',
                type: 'delete',
                key: 'object',
                md: {
                    ...objMd,
                    versionId: '98500086134471999999RG001  0',
                    isNull: true,
                },
                getAccountIdResponse: [null, '1234'],
                called: true,
            },
            {
                it: 'should still push when we fail to get accountId',
                type: 'delete',
                key: 'object',
                md: {
                    ...objMd,
                    versionId: '98500086134471999999RG001  0',
                    isNull: true,
                },
                getAccountIdResponse: [errors.InternalError, ''],
                called: true,
            },
            {
                it: 'should ignore if producer not found',
                type: 'delete',
                key: 'object',
                md: {
                    ...objMd,
                    'x-amz-storage-class': 'dmf-v2',
                    'dataStoreName': 'dmf-v2',
                },
                getAccountIdResponse: [null, ''],
                called: false,
            },
        ].forEach(params => {
            it(params.it, () => {
                lcqp.vaultClientWrapper = {
                    getAccountId: sinon.stub().yields(...params.getAccountIdResponse),
                };
                const timestamp = new Date();
                const entry = {
                    type: params.type,
                    bucket: 'lc-queue-populator-test-bucket',
                    key: params.key,
                    value: JSON.stringify(params.md),
                    overheadFields: {
                        commitTimestamp: timestamp,
                    },
                };
                lcqp._handleDeleteOp(entry);
                assert.strictEqual(kafkaSendStub.calledOnce, params.called);
                if (!params.called) {
                    return;
                }
                const message = JSON.parse(kafkaSendStub.args[0][0][0].message);
                const expectedMessage = {
                    bucketName: 'lc-queue-populator-test-bucket',
                    objectKey: params.md.key,
                    archiveInfo: params.md.archive.archiveInfo,
                    requestId: message.requestId,
                    transitionTime: timestamp.toISOString(),
                    accountId: params.getAccountIdResponse[1],
                };
                if (params.md.versionId) {
                    expectedMessage.objectVersion = encode(params.md.versionId);
                }
                assert.deepStrictEqual(message, expectedMessage);
            });
        });
    });
});
