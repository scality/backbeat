const assert = require('assert');

const { prettyDuration } = require('../../../../lib/util/metrics');

describe('prettyDuration', () => {
    it('should convert raw seconds to pretty-printed approximation', () => {
        assert.strictEqual(prettyDuration(0), '0 seconds');
        assert.strictEqual(prettyDuration(10), '10 seconds');
        assert.strictEqual(prettyDuration(120), '120 seconds');
        assert.strictEqual(prettyDuration(360), '6 minutes');
        assert.strictEqual(prettyDuration(359), '6 minutes');
        assert.strictEqual(prettyDuration(361.5), '6 minutes');
        assert.strictEqual(prettyDuration(7200), '120 minutes');
        assert.strictEqual(prettyDuration(36000), '10 hours');
        assert.strictEqual(prettyDuration(864000), '10 days');
        assert.strictEqual(prettyDuration(-42), 'invalid (-42 seconds)');
    });
});
