const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');

const config = require('../../../lib/Config');
const { coldStorageRestoreTopicPrefix } = config.extensions.lifecycle;

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
        it('should have one producer per cold location', done => {
            lcqp.locationConfigs = Object.assign({}, locationConfigs, coldLocationConfigs);
            lcqp.setupProducers(() => {
                const producers = Object.keys(lcqp._producers);
                const coldLocations = Object.keys(coldLocationConfigs);
                assert.strictEqual(producers.length, coldLocations.length);
                coldLocations.forEach(loc => {
                    const topic = `${coldStorageRestoreTopicPrefix}${loc}`;
                    assert(producers.includes(topic));
                });
                done();
            });
        });
    });

    describe(':_handleRestoreOp', () => {
        let lcqp;
        beforeEach(() => {
            lcqp = new LifecycleQueuePopulator(params);
            lcqp.locationConfigs = Object.assign({}, coldLocationConfigs, locationConfigs);
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
                event: 's3:ObjectRestore',
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
    });
});
