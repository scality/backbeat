const assert = require('assert');

const { buildDeliveryKey }
    = require('../../../../extensions/notification/utils/deliveryKey');

describe('deliveryKey ::', () => {
    it('should use the destination resource when no spread factor is set', () => {
        const key = buildDeliveryKey({ resource: 'destination1' },
            'example-bucket', 'example-key');
        assert.strictEqual(key, 'destination1');
    });

    it('should use the destination resource when the spread factor is 1', () => {
        const key = buildDeliveryKey({ resource: 'destination1', spreadFactor: 1 },
            'example-bucket', 'example-key');
        assert.strictEqual(key, 'destination1');
    });

    it('should keep the key stable for the same bucket and object', () => {
        const destination = { resource: 'destination1', spreadFactor: 4 };
        const key = buildDeliveryKey(destination, 'example-bucket', 'example-key');
        for (let i = 0; i < 10; i++) {
            assert.strictEqual(
                buildDeliveryKey(destination, 'example-bucket', 'example-key'), key);
        }
    });

    it('should keep the spread index within the spread factor bounds', () => {
        const destination = { resource: 'destination1', spreadFactor: 4 };
        for (let i = 0; i < 100; i++) {
            const key = buildDeliveryKey(destination, 'example-bucket', `example-key-${i}`);
            const [resource, index] = key.split('|');
            assert.strictEqual(resource, 'destination1');
            assert(Number.isInteger(Number(index)));
            assert(Number(index) >= 0 && Number(index) < 4);
        }
    });

    it('should spread the objects of a destination over several keys', () => {
        const destination = { resource: 'destination1', spreadFactor: 4 };
        const keys = new Set();
        for (let i = 0; i < 100; i++) {
            keys.add(buildDeliveryKey(destination, 'example-bucket', `example-key-${i}`));
        }
        assert(keys.size > 1);
    });

    it('should give two destinations different keys for the same object', () => {
        const first = buildDeliveryKey({ resource: 'destination1', spreadFactor: 4 },
            'example-bucket', 'example-key');
        const second = buildDeliveryKey({ resource: 'destination2', spreadFactor: 4 },
            'example-bucket', 'example-key');
        assert.notStrictEqual(first, second);
    });
});
