const assert = require('assert');

const runTasksWithConcurrency =
      require('../../../../lib/util/runTasksWithConcurrency');

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

describe('runTasksWithConcurrency', () => {
    it('should process an empty array', async () => {
        const [err, results] = await runTasksWithConcurrency(
            [], 10, async item => item * 2);
        assert.ifError(err);
        assert.deepStrictEqual(results, []);
    });

    [
        { arrayDesc: 'smaller than the concurrency limit', limit: 10 },
        { arrayDesc: 'equal to the concurrency limit',    limit: 5  },
        { arrayDesc: 'larger than the concurrency limit', limit: 3  },
        { arrayDesc: 'with a concurrency limit of 1',     limit: 1  },
    ].forEach(testCase => {
        it(`should process an array ${testCase.arrayDesc}`, async () => {
            let concurrency = 0;
            const [err, results] = await runTasksWithConcurrency(
                [1, 2, 3, 4, 5], testCase.limit, async item => {
                    concurrency++;
                    assert(concurrency <= testCase.limit);
                    await delay(Math.random() * 10);
                    concurrency--;
                    return item * 2;
                });
            assert.ifError(err);
            assert.deepStrictEqual(results, [2, 4, 6, 8, 10]);
        });
    });

    it('should launch tasks in parallel up to the concurrency limit', async () => {
        let concurrency = 0;
        const pendingResolvers = [];
        let scheduled = false;

        const [err, results] = await runTasksWithConcurrency(
            [1, 2, 3, 4, 5], 3, item => {
                concurrency++;
                assert(concurrency <= 3);
                return new Promise(resolve => {
                    const settle = () => {
                        concurrency--;
                        resolve(item * 2);
                    };
                    if (!scheduled) {
                        pendingResolvers.push(settle);
                        if (concurrency === 3) {
                            scheduled = true;
                            setTimeout(() => pendingResolvers.splice(0).forEach(fn => fn()), 10);
                        }
                    } else {
                        settle();
                    }
                });
            });
        assert.ifError(err);
        assert.deepStrictEqual(results, [2, 4, 6, 8, 10]);
    });

    it('should stop processing new tasks on error', async () => {
        const processed = [];
        const [err, results] = await runTasksWithConcurrency(
            [1, 2, 3, 4, 5], 1, async item => {
                processed.push(item);
                if (item === 3) {
                    throw new Error('OOPS');
                }
                return item * 2;
            });
        assert(err);
        assert.strictEqual(err.message, 'OOPS');
        // items 4 and 5 must not have been started
        assert.deepStrictEqual(processed, [1, 2, 3]);
        assert.strictEqual(results[0], 2);
        assert.strictEqual(results[1], 4);
        assert.strictEqual(results[2], undefined); // errored item has no result
    });

    it('should finish all pending tasks on error', async () => {
        let concurrency = 0;
        const pendingResolvers = [];

        const [err, results] = await runTasksWithConcurrency(
            [1, 2, 3, 4, 5], 5, item => {
                concurrency++;
                if (concurrency === 5) {
                    setTimeout(() => pendingResolvers.splice(0).forEach(fn => fn()), 10);
                    return Promise.reject(new Error('OOPS'));
                }
                return new Promise(resolve =>
                    pendingResolvers.push(() => resolve(item * 2)));
            });
        assert(err);
        assert.strictEqual(err.message, 'OOPS');
        // items 1-4 completed despite item 5 erroring first
        assert.strictEqual(results[0], 2);
        assert.strictEqual(results[1], 4);
        assert.strictEqual(results[2], 6);
        assert.strictEqual(results[3], 8);
        assert.strictEqual(results[4], undefined); // errored item has no result
    });

    it('should return the first error (consistent with original behavior)', async () => {
        const [err, results] = await runTasksWithConcurrency(
            [1, 2, 3, 4, 5], 5, item => {
                if (item === 3) {
                    return Promise.reject(new Error(`OOPS ${item}`));
                }
                return delay(10).then(() => { throw new Error(`OOPS ${item}`); });
            });
        assert(err);
        // item 3 rejects first (no delay), all 5 were already in-flight
        assert.strictEqual(err.message, 'OOPS 3');
        assert.strictEqual(results.length, 5);
        results.forEach(r => assert.strictEqual(r, undefined));
    });
});
