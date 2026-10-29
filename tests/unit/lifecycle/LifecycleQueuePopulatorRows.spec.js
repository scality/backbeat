const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');

const config = require('../../../lib/Config');
const { coldStorageGCTopicPrefix } = config.extensions.lifecycle;
const LifecycleQueuePopulator = require('../../../extensions/lifecycle/LifecycleQueuePopulator');
const { listEntries } = require('../../utils/nullVersionRows');

const logger = new werelogs.Logger('test:LifecycleQueuePopulator');

const locationConfigs = {
    'dmf-v1': { isCold: true, type: 'tlp' },
    'us-east-1': { type: 'aws_s3' },
};

const archivedMd = {
    'md-model-version': 2,
    'owner-id': '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
    'x-amz-storage-class': 'dmf-v1',
    'content-length': 542,
    'content-md5': '01064f35c238bd2b785e34508c3d27f4',
    'last-modified': '2017-07-13T02:44:25.519Z',
    'key': 'obj',
    'location': [],
    'archive': {
        archiveInfo: {
            archiveId: '04425717-a65c-4e8a-95e1-fa1d902d9d9f',
            archiveVersion: 7504504064263669,
        },
    },
    'dataStoreName': 'dmf-v1',
};

// master versus version key entries of each layout, in the lifecycle
// handlers, which skip delete markers on top of the shared rule
describe('LifecycleQueuePopulator: master versus version entries', () => {
    let lcqp;

    beforeEach(() => {
        lcqp = new LifecycleQueuePopulator({ authConfig: { transport: 'http' }, logger });
        lcqp.locationConfigs = locationConfigs;
    });

    describe('_isDistinctObjectEntry', () => {
        listEntries().forEach(({ desc, key, value, shadowed }) => {
            const expected = !shadowed && !value.isDeleteMarker;
            it(`should return ${expected} for ${desc}`, () => {
                const entry = { type: 'put', bucket: 'bucket', key, value: JSON.stringify(value) };
                assert.strictEqual(lcqp._isDistinctObjectEntry(entry, value), expected);
            });
        });
    });

    describe('_handleDeleteOp', () => {
        let kafkaSendStub;

        beforeEach(() => {
            kafkaSendStub = sinon.stub().yields();
            lcqp._producers[`${coldStorageGCTopicPrefix}dmf-v1`] = { send: kafkaSendStub };
            lcqp.vaultClientWrapper = { getAccountId: sinon.stub().yields(null, '1234') };
        });

        listEntries(archivedMd).forEach(({ desc, key, value, shadowed }) => {
            const expected = !shadowed && !value.isDeleteMarker;
            it(`should ${expected ? 'publish a GC request' : 'skip'} for ${desc}`, () => {
                lcqp._handleDeleteOp({
                    type: 'delete',
                    bucket: 'bucket',
                    key,
                    value: JSON.stringify(value),
                    overheadFields: { commitTimestamp: Date.now() },
                });
                assert.strictEqual(kafkaSendStub.calledOnce, expected);
            });
        });
    });
});
