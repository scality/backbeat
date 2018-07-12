const assert = require('assert');
const ReplicateObject =
    require('../../../extensions/replication/tasks/ReplicateObject');

const partSize = 1024 * 1024 * 1024 + 1;
const MPU_GCP_MAX_PARTS = 1024;

describe('ReplicateObject', () => {
    const task = new ReplicateObject({
        getStateVars: () => ({
            repConfig: {
                queueProcessor: {
                    retryTimeoutS: 0,
                },
            },
        }),
    });

    describe('::_getRanges', () => {
        it('should get a list of ranges with content length 5MB', () => {
            const contentLength = 1024 * 1024 * 5;
            const ranges = task._getRanges(contentLength, false);
            assert.strictEqual(1, ranges.length);
            const expected = { start: 0, end: contentLength - 1 };
            assert.deepStrictEqual(ranges[0], expected);
        });

        it('should get a list of ranges with content length 8388608', () => {
            const contentLength = 8388608;
            const ranges = task._getRanges(contentLength, false);
            assert.strictEqual(1, ranges.length);
            const expected = { start: 0, end: contentLength - 1 };
            assert.deepStrictEqual(ranges[0], expected);
        });

        it('should get a list of ranges with content length 8388609', () => {
            const ranges = task._getRanges(8388609, false);
            assert.strictEqual(2, ranges.length);
            let expected = { start: 0, end: 8388607 };
            assert.deepStrictEqual(ranges[0], expected);
            expected = { start: 8388608, end: 8388608 };
            assert.deepStrictEqual(ranges[1], expected);
        });

        it('should get a list of ranges with content length 5GB', () => {
            const contentLength = 1024 * 1024 * 1024 * 5;
            const ranges = task._getRanges(contentLength, false);
            assert.strictEqual(640, ranges.length);
            let expected = { start: 0, end: 8388607 };
            assert.deepStrictEqual(ranges[0], expected);
            expected = { start: 8388608, end: 16777215 };
            assert.deepStrictEqual(ranges[1], expected);
            expected = { start: 5360320512, end: 5368709119 };
            assert.deepStrictEqual(ranges[ranges.length - 1], expected);
        });

        it('should get a list of ranges with content length 5T', () => {
            const contentLength = 1024 * 1024 * 1024 * 1024 * 5; // 5TB
            const ranges = task._getRanges(contentLength, false);
            assert.strictEqual(5120, ranges.length);
            let expected = { start: 0, end: 1073741823 };
            assert.deepStrictEqual(ranges[0], expected);
            expected = { start: 1073741824, end: 2147483647 };
            assert.deepStrictEqual(ranges[1], expected);
            expected = { start: 5496484397056, end: 5497558138879 };
            assert.deepStrictEqual(ranges[ranges.length - 1], expected);
        });

        it('should ensure all parts of the original object are intact', () => {
            const contentLengths = [];
            for (let i = 1; i <= 1024 * 1024 * 1024 * 1024 * 5; i *= 2) {
                contentLengths.push(i);
            }
            contentLengths.forEach(contentLength => {
                const minPartSize = 1024 * 1024 * 5; // 5MB
                const maxPartSize = 1024 * 1024 * 1024 * 5; // 5GB
                const ranges = task._getRanges(contentLength, false);
                assert(ranges.length <= 10000);
                let sum = 0;
                for (let i = 0; i < ranges.length; i++) {
                    const { start, end } = ranges[i];
                    const rangeSize = end - start + 1; // Range is inclusive.
                    const isLastPart = i + 1 === ranges.length;
                    assert(rangeSize >= isLastPart ? 1 : minPartSize);
                    assert(rangeSize <= maxPartSize);
                    // Assert that the range size is a power of two.
                    if (!isLastPart) {
                        assert(Math.ceil(Math.log2(rangeSize)) ===
                            Math.floor(Math.log2(rangeSize)));
                    }
                    sum += rangeSize;
                }
                assert(sum === contentLength);
            });
        });

        it('should get <= 1024 ranges for part count 1025-10000', () => {
            Array.from(Array(10000 - 1024).keys()).forEach(n => {
                const count = n + 1025;
                const ranges = task._getRanges(count * partSize, true);
                const contentLen = count * partSize;
                const pow = Math.pow(2,
                    Math.ceil(Math.log(contentLen) / Math.log(2)));
                const range = pow / MPU_GCP_MAX_PARTS;
                const msg = `incorrect value for part count: ${count}`;
                assert.strictEqual(ranges.length <= 1024, true, msg);
                assert.deepStrictEqual(ranges[0], {
                    start: 0,
                    end: range - 1,
                }, msg);
                assert.deepStrictEqual(ranges[1], {
                    start: range,
                    end: range * 2 - 1,
                }, msg);
                assert.deepStrictEqual(ranges[ranges.length - 1], {
                    start: range * (ranges.length - 1),
                    end: contentLen - 1,
                }, msg);
            });
        });
    });
});
