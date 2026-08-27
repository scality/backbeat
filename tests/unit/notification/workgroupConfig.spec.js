const assert = require('assert');
const sinon = require('sinon');

const FakeLogger = require('../../utils/fakeLogger');

const { DELIVERY_POOL_WORKGROUP_ID_ENV, resolveWorkgroupId } = require(
    '../../../extensions/notification/deliveryWorker/workgroupConfig');

const ENV = DELIVERY_POOL_WORKGROUP_ID_ENV;

const noWorkgroups = {
    enabled: true,
    topic: 'delivery-topic',
    groupId: 'delivery-group',
};

const withId = {
    ...noWorkgroups,
    workgroups: {
        id: 'wg-configured',
        zookeeperPath: '/notification/delivery-workgroups',
        cachePath: '/tmp/backbeat-delivery-workgroups.json',
    },
};

const withoutId = {
    ...noWorkgroups,
    workgroups: {
        zookeeperPath: '/notification/delivery-workgroups',
        cachePath: '/tmp/backbeat-delivery-workgroups.json',
    },
};

describe('notification workgroup identity ::', () => {
    let logger;

    beforeEach(() => {
        logger = { ...FakeLogger, warn: sinon.stub() };
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should pin the environment variable name', () => {
        assert.strictEqual(DELIVERY_POOL_WORKGROUP_ID_ENV,
            'DELIVERY_POOL_WORKGROUP_ID');
    });

    it('should stay undefined and quiet when workgroups are not configured', () => {
        assert.strictEqual(
            resolveWorkgroupId(noWorkgroups, {}, logger), undefined);
        assert.strictEqual(resolveWorkgroupId(undefined, {}, logger), undefined);
        assert(logger.warn.notCalled);
    });

    it('should warn about an id from the environment with no workgroups', () => {
        assert.strictEqual(
            resolveWorkgroupId(noWorkgroups, { [ENV]: 'wg-env' }, logger),
            undefined);
        assert(logger.warn.calledOnce);
        assert.strictEqual(logger.warn.args[0][1].value, 'wg-env');
    });

    it('should keep the configured id when the environment is silent', () => {
        assert.strictEqual(
            resolveWorkgroupId(withId, {}, logger), 'wg-configured');
        assert(logger.warn.notCalled);
    });

    it('should keep the configured id quietly for an empty override', () => {
        ['', '   '].forEach(value => {
            assert.strictEqual(
                resolveWorkgroupId(withId, { [ENV]: value }, logger),
                'wg-configured', `for value "${value}"`);
        });
        assert(logger.warn.notCalled);
    });

    it('should let the environment give this worker its workgroup', () => {
        assert.strictEqual(
            resolveWorkgroupId(withId, { [ENV]: '  wg-env  ' }, logger),
            'wg-env');
        assert(logger.warn.notCalled);
    });

    it('should fall back to the configured id and warn for a bad override', () => {
        ['-wg-env', 'wg env', 'wg.env', 'a'.repeat(65)].forEach(value => {
            logger.warn.resetHistory();
            assert.strictEqual(
                resolveWorkgroupId(withId, { [ENV]: value }, logger),
                'wg-configured', `for value "${value}"`);
            assert(logger.warn.calledOnce, `no warning for value "${value}"`);
            assert.strictEqual(logger.warn.args[0][1].value, value);
        });
    });

    it('should take the environment id when none is configured', () => {
        assert.strictEqual(
            resolveWorkgroupId(withoutId, { [ENV]: 'wg-env' }, logger),
            'wg-env');
        assert(logger.warn.notCalled);
    });

    it('should stay undefined when neither config nor environment has an id', () => {
        assert.strictEqual(
            resolveWorkgroupId(withoutId, {}, logger), undefined);
        assert(logger.warn.notCalled);

        assert.strictEqual(
            resolveWorkgroupId(withoutId, { [ENV]: 'not a workgroup' }, logger),
            undefined);
        assert(logger.warn.calledOnce);
    });

    it('should never throw without a logger', () => {
        assert.doesNotThrow(() => {
            resolveWorkgroupId(withId, { [ENV]: 'wg env' });
            resolveWorkgroupId(noWorkgroups, { [ENV]: 'wg-env' });
            resolveWorkgroupId(withId);
        });
    });
});
