const assert = require('assert');

const IngestionQueuePopulator =
    require('../../../extensions/ingestion/IngestionQueuePopulator');

const fakeLogger = require('../../utils/fakeLogger');

describe('ingestion queue populator', () => {
    let iqp;

    beforeEach(() => {
        const params = {
            config: { topic: 'test-topic' },
            logger: fakeLogger,
            instanceId: 'test-instance',
        };
        iqp = new IngestionQueuePopulator(params);
    });

    // Regression test: a malformed raft entry value must not
    // crash the populator. Before this fix, `JSON.parse(entry.value)` at the
    // top of `_filterValueOp` threw a SyntaxError on malformed input, which
    // propagated up through the synchronous filter chain and exited the
    // populator process. The fix wraps the parse with `safeJsonParse`, logs
    // at error level with the entry's identifying context, and returns true
    // so the caller skips publishing this entry.
    it('should not throw on malformed entry value and should signal skip', () => {
        const entry = {
            type: 'put',
            bucket: 'test-bucket',
            key: 'test-key',
            // malformed JSON — closing brace missing mid-stream; same shape as
            // observed upstream of the replication populator (RD-307).
            value: '{"owner-display-name":"x","content-length":42,"acl":{"Canned":"p"',
        };

        let result;
        assert.doesNotThrow(() => {
            result = iqp._filterValueOp(entry);
        });
        // `_filterValueOp` returns true when the caller should skip this entry
        assert.strictEqual(result, true);
    });
});
