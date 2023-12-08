const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');
const { encode } = require('arsenal').versioning.VersionID;

const config = require('../../../lib/Config');
const {
    coldStorageRestoreAdjustTopicPrefix,
    coldStorageRestoreTopicPrefix,
    coldStorageGCTopicPrefix
} = config.extensions.lifecycle;

const LifecycleQueuePopulator = require('../../../extensions/lifecycle/LifecycleQueuePopulator');

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
        type: 'dmf',
    },
    'dmf-v2': {
        isCold: true,
        type: 'dmf',
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
        it('should have three producers per cold location', done => {
            lcqp.locationConfigs = Object.assign({}, locationConfigs, coldLocationConfigs);
            lcqp.setupProducers(() => {
                const producers = Object.keys(lcqp._producers);
                const coldLocations = Object.keys(coldLocationConfigs);
                assert.strictEqual(producers.length, coldLocations.length * 3);
                coldLocations.forEach(loc => {
                    assert(producers.includes(`${coldStorageRestoreAdjustTopicPrefix}${loc}`));
                    assert(producers.includes(`${coldStorageRestoreTopicPrefix}${loc}`));
                    assert(producers.includes(`${coldStorageGCTopicPrefix}${loc}`));
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

    describe(':filter', () => {
        let lcqp;
        beforeEach(() => {
            lcqp = new LifecycleQueuePopulator(params);
            lcqp.locationConfigs = Object.assign({}, coldLocationConfigs, locationConfigs);
        });
        it('it should call _handleDeleteOp on delete message', () => {
            const handleDeleteStub = sinon.stub(lcqp, '_handleDeleteOp').returns();
            lcqp.filter({
                type: 'delete',
                bucket: 'lc-queue-populator-test-bucket',
                key: 'hosts\x0098500086134471999999RG001  0',
                value: JSON.stringify(templateEntry),
            });
            assert(handleDeleteStub.calledOnce);
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
                called: true,
            },
            {
                it: 'should process non versioned master',
                type: 'delete',
                key: 'object',
                md: {
                    ...objMd,
                },
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
                called: true,
            },
        ].forEach(params => {
            it(params.it, () => {
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
                };
                if (params.md.versionId) {
                    expectedMessage.objectVersion = encode(params.md.versionId);
                }
                assert.deepStrictEqual(message, expectedMessage);
            });
        });
    });
});
