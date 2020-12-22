const assert = require('assert');

const mapLimitWaitPendingIfError =
      require('../../../../lib/util/mapLimitWaitPendingIfError');

describe('mapLimitWaitPendingIfError', () => {
    it('should process an empty array', done => {
        mapLimitWaitPendingIfError([], 10, (item, itemCb) => {
            setTimeout(() => itemCb(null, item * 2), Math.random() * 10);
        }, (err, results) => {
            assert.ifError(err);
            assert.deepStrictEqual(results, []);
            done();
        });
    });
    [
        {
            arrayDesc: 'smaller than the concurrency limit',
            limit: 10,
        },
        {
            arrayDesc: 'equal to the concurrency limit',
            limit: 5,
        },
        {
            arrayDesc: 'larger than the concurrency limit',
            limit: 3,
        },
        {
            arrayDesc: 'with a concurrency limit of 1',
            limit: 1,
        },
    ].forEach(testCase => {
        it(`should process an array ${testCase.arrayDesc}`, done => {
            let concurrency = 0;
            mapLimitWaitPendingIfError([1, 2, 3, 4, 5], testCase.limit, (item, itemCb) => {
                concurrency += 1;
                assert(concurrency <= testCase.limit);
                setTimeout(() => {
                    concurrency -= 1;
                    itemCb(null, item * 2);
                }, Math.random() * 10);
            }, (err, results) => {
                assert.ifError(err);
                assert.deepStrictEqual(results, [2, 4, 6, 8, 10]);
                done();
            });
        });
    });
    it('should launch tasks in parallel up to the concurrency limit', done => {
        let concurrency = 0;
        const cbs = [];
        let testDone = false;
        mapLimitWaitPendingIfError([1, 2, 3, 4, 5], 3, (item, itemCb) => {
            concurrency += 1;
            assert(concurrency <= 3);
            const itemDone = () => {
                concurrency -= 1;
                process.nextTick(() => itemCb(null, item * 2));
            };
            if (testDone) {
                itemDone();
            } else {
                cbs.push(itemDone);
                if (concurrency === 3) {
                    setTimeout(() => {
                        cbs.forEach(cb => cb());
                    }, 10);
                    testDone = true;
                }
            }
        }, (err, results) => {
            assert.ifError(err);
            assert.deepStrictEqual(results, [2, 4, 6, 8, 10]);
            done();
        });
    });

    it('should stop processing new requests on error', done => {
        mapLimitWaitPendingIfError([1, 2, 3, 4, 5], 1, (item, itemCb) => {
            // check that no more item is processed after an error
            // occurs (limit is 1 so item are processed in order)
            assert(item <= 3);
            if (item === 3) {
                process.nextTick(() => itemCb(new Error('OOPS'), 'error item'));
            } else {
                process.nextTick(() => itemCb(null, item * 2));
            }
        }, (err, results) => {
            assert(err);
            assert.deepStrictEqual(results, [2, 4, 'error item']);
            done();
        });
    });

    it('should finish all pending requests on error', done => {
        let concurrency = 0;
        const cbs = [];
        mapLimitWaitPendingIfError([1, 2, 3, 4, 5], 5, (item, itemCb) => {
            concurrency += 1;
            if (concurrency === 5) {
                process.nextTick(() => {
                    itemCb(new Error('OOPS'), 'error item');
                    setTimeout(() => {
                        cbs.forEach(cb => cb());
                    }, 10);
                });
            } else {
                cbs.push(() => itemCb(null, item * 2));
            }
        }, (err, results) => {
            assert(err);
            assert.deepStrictEqual(results, [2, 4, 6, 8, 'error item']);
            done();
        });
    });

    it('should return the first error', done => {
        mapLimitWaitPendingIfError([1, 2, 3, 4, 5], 5, (item, itemCb) => {
            const errorCb = () => itemCb(
                new Error(`OOPS ${item}`), `error item ${item}`);
            if (item === 3) {
                process.nextTick(errorCb);
            } else {
                setTimeout(errorCb, 10);
            }
        }, (err, results) => {
            assert(err);
            assert.strictEqual(err.message, 'OOPS 3');
            assert.deepStrictEqual(results, [
                'error item 1',
                'error item 2',
                'error item 3',
                'error item 4',
                'error item 5',
            ]);
            done();
        });
    });
});
