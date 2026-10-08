const assert = require('assert');

const {
    timestampFromResumeToken,
    timestampFromSourceOffset,
    timestampFromStartupTime,
    formatStartupTime,
    compareTimestamps,
    minTimestamp,
} = require('../../../extensions/oplogPopulator/modules/resumeToken');

// cluster time (t: 0x6ac50e0c, i: 3) followed by the rest of the KeyString
const eventToken = {
    _data: '826AC50E0C000000032B042C0100296E5A10046C2C6A1B4F6C4A9E8D' +
        '4B1D7E0B0B9C4F463C6F7065726174696F6E54797065003C696E7365727400' +
        '46646F63756D656E744B65790046645F6964006466AF0E0C2A5D0B1A2C3D4E5F000004',
};
// postBatchResumeToken, as committed by heartbeats: high water mark, same
// leading cluster time
const heartbeatToken = { _data: '826AC50E0D000000012B0429296E1404' };

describe('resumeToken', () => {
    describe('timestampFromResumeToken', () => {
        it('should decode the cluster time of an event token', () => {
            assert.deepStrictEqual(timestampFromResumeToken(eventToken),
                { t: 0x6ac50e0c, i: 3 });
        });

        it('should decode the cluster time of a heartbeat token', () => {
            assert.deepStrictEqual(timestampFromResumeToken(heartbeatToken),
                { t: 0x6ac50e0d, i: 1 });
        });

        it('should decode unsigned values', () => {
            assert.deepStrictEqual(timestampFromResumeToken({ _data: '82FFFFFFFFFFFFFFFF04' }),
                { t: 0xffffffff, i: 0xffffffff });
        });

        [
            ['missing token', undefined],
            ['missing data', {}],
            ['binary data', { _data: { $binary: 'AA==' } }],
            ['other leading type', { _data: '6E6AC50E0C00000003' }],
            ['truncated data', { _data: '826AC50E' }],
        ].forEach(([desc, token]) => {
            it(`should return null on ${desc}`, () => {
                assert.strictEqual(timestampFromResumeToken(token), null);
            });
        });
    });

    describe('timestampFromSourceOffset', () => {
        it('should decode an event offset', () => {
            assert.deepStrictEqual(
                timestampFromSourceOffset({ _id: JSON.stringify(eventToken) }),
                { t: 0x6ac50e0c, i: 3 });
        });

        it('should decode a heartbeat offset', () => {
            assert.deepStrictEqual(
                timestampFromSourceOffset({ _id: JSON.stringify(heartbeatToken), HEARTBEAT: 'true' }),
                { t: 0x6ac50e0d, i: 1 });
        });

        it('should return null on unparsable offsets', () => {
            assert.strictEqual(timestampFromSourceOffset({ _id: '{"_data":' }), null);
            assert.strictEqual(timestampFromSourceOffset({}), null);
            assert.strictEqual(timestampFromSourceOffset(null), null);
        });
    });

    describe('timestampFromStartupTime', () => {
        it('should parse extended JSON timestamps', () => {
            assert.deepStrictEqual(timestampFromStartupTime(formatStartupTime({ t: 12, i: 3 })),
                { t: 12, i: 3 });
        });

        it('should parse ISO dates, truncating to the second', () => {
            assert.deepStrictEqual(timestampFromStartupTime('1970-01-01T00:00:12.999Z'),
                { t: 12, i: 0 });
        });

        it('should parse epoch seconds', () => {
            assert.deepStrictEqual(timestampFromStartupTime('12'), { t: 12, i: 0 });
        });

        it('should return null when unset or invalid', () => {
            assert.strictEqual(timestampFromStartupTime(undefined), null);
            assert.strictEqual(timestampFromStartupTime(''), null);
            assert.strictEqual(timestampFromStartupTime('not a date'), null);
        });
    });

    describe('compareTimestamps / minTimestamp', () => {
        it('should order by seconds then increment', () => {
            assert(compareTimestamps({ t: 1, i: 9 }, { t: 2, i: 0 }) < 0);
            assert(compareTimestamps({ t: 2, i: 1 }, { t: 2, i: 0 }) > 0);
            assert.strictEqual(compareTimestamps({ t: 2, i: 1 }, { t: 2, i: 1 }), 0);
        });

        it('should return the earliest timestamp', () => {
            assert.deepStrictEqual(minTimestamp([{ t: 2, i: 0 }, { t: 1, i: 5 }, { t: 1, i: 4 }]),
                { t: 1, i: 4 });
            assert.strictEqual(minTimestamp([]), null);
        });
    });
});
