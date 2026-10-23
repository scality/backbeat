const assert = require('assert');

const IngestionQueuePopulator =
    require('../../../extensions/ingestion/IngestionQueuePopulator');

const fakeLogger = require('../../utils/fakeLogger');
const { listEntries } = require('../../utils/nullVersionRows');

describe('ingestion queue populator', () => {
    let iqp;

    beforeEach(() => {
        const params = {
            config: { topic: 'test-topic' },
            logger: fakeLogger,
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

    describe('master versus version entries', () => {
        const baseValue = {
            'owner-display-name': 'test',
            'owner-id': 'e166a2080a0c2cf1474dce54654f3f224dd5ae01379f20f338d106b8bc964bb1',
            'content-length': 128,
            'content-md5': 'd41d8cd98f00b204e9800118ecf8427e',
            'key': '',
            'location': null,
            'last-modified': '2018-03-28T22:10:00.534Z',
            'md-model-version': 3,
        };

        listEntries(baseValue).forEach(({ desc, key, value, shadowed }) => {
            it(`should ${shadowed ? 'skip' : 'process'} ${desc}`, () => {
                const entry = {
                    type: 'put',
                    bucket: 'test-bucket',
                    key,
                    value: JSON.stringify(value),
                };
                assert.strictEqual(iqp._filterValueOp(entry), shadowed);
            });
        });
    });
});
