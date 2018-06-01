const assert = require('assert');
const MultipleBackendTask =
    require('../../../extensions/replication/tasks/MultipleBackendTask');

const partSize = 1024 * 1024 * 1024 + 1;
const MPU_GCP_MAX_PARTS = 1024;

describe('MultipleBackendTask', () => {
    const task = new MultipleBackendTask({
        getStateVars: () => ({
            repConfig: {
                queueProcessor: {
                    retryTimeoutS: 0,
                },
            },
        }),
    });

    describe('::_getRanges', () => {
        it('should get a list of ranges with content length 1025', () => {
            const ranges = task._getRanges(1025);
            assert.strictEqual(513, ranges.length);
            assert.deepStrictEqual(ranges[0], {
                start: 0,
                end: 1,
            });
            assert.deepStrictEqual(ranges[1], {
                start: 2,
                end: 3,
            });
            assert.deepStrictEqual(ranges[ranges.length - 1], {
                start: 1024,
                end: 1024,
            });
        });

        it('should get a list of ranges with content length 1026', () => {
            const ranges = task._getRanges(1026);
            assert.deepStrictEqual(ranges[0], {
                start: 0,
                end: 1,
            });
            assert.deepStrictEqual(ranges[1], {
                start: 2,
                end: 3,
            });
            assert.deepStrictEqual(ranges[ranges.length - 1], {
                start: 1024,
                end: 1025,
            });
        });

        it('should get <= 1024 ranges for part count 1025-10000', () => {
            Array.from(Array(10000 - 1024).keys()).forEach(n => {
                const count = n + 1025;
                const ranges = task._getRanges(count * partSize);
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
