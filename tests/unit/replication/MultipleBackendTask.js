const assert = require('assert');
const jsutil = require('arsenal').jsutil;

const config = require('../../config.json');
const MultipleBackendTask =
    require('../../../extensions/replication/tasks/MultipleBackendTask');
const QueueEntry = require('../../../lib/models/QueueEntry');
const { sourceEntry } = require('../../utils/mockEntries');
const fakeLogger = require('../../utils/fakeLogger');
const { replicationEntry } = require('../../utils/kafkaEntries');

const MPU_GCP_MAX_PARTS = 1024;
const MIN_AWS_PART_SIZE = (1024 * 1024) * 5; // 5MB
const MAX_AWS_PART_SIZE = (1024 * 1024 * 1024) * 5; // 5GB
const MAX_AWS_OBJECT_SIZE = (1024 * 1024 * 1024 * 1024) * 5; // 5TB
const retryConfig = { scality: { timeoutS: 300 } };

describe('MultipleBackendTask', function test() {
    this.timeout(5000);
    let task;

    function checkPartLength(contentLength, expectedPartSize) {
        const partSize = task._getRangeSize(contentLength);
        assert.strictEqual(partSize, expectedPartSize);
    }

    function requestInitiateMPU(params, done) {
        const { retryable } = params;

        task.backbeatSource = {
            multipleBackendInitiateMPU: () => ({
                httpRequest: { headers: {} },
                send: cb => cb({ retryable }),
                on: (action, cb) => cb(),
            }),
        };

        task._getAndPutMultipartUpload(sourceEntry, fakeLogger, err => {
                assert(err);
                return done();
            });
    }

    beforeEach(() => {
        task = new MultipleBackendTask({
            getStateVars: () => ({
                repConfig: {
                    queueProcessor: {
                        retry: retryConfig,
                    },
                },
                sourceConfig: config.extensions.replication.source,
                destConfig: config.extensions.replication.destination,
                site: 'test-site-2',
            }),
        });
    });

    describe('::_setupClients', () => {
        it('should set client for replication', done => {
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            task._setupClients(entry, fakeLogger, () => {
                assert(task.sourceRole !== null);
                assert(task.S3source !== null);
                assert(task.backbeatSource !== null);
                assert(task.backbeatSourceProxy !== null);
                done();
            });
        });
    });

    describe('::initiateMultipartUpload', () => {
        it('should use exponential backoff if retryable error ', done => {
            const doneOnce = jsutil.once(done);
            setTimeout(() => {
                // inhibits further retries
                task.config.retryTimeoutS = 0;
                doneOnce();
            }, 4000); // Retries will exceed test timeout.
            requestInitiateMPU({ retryable: true }, doneOnce);
        });

        it('should not use exponential backoff if non-retryable error ', done =>
            requestInitiateMPU({ retryable: false }, done));
    });

    describe('::_getRangeSize', () => {
        it('should get correct part sizes', () => {
            checkPartLength(0, 0);
            checkPartLength(1, 1);
            checkPartLength((1024 * 1024) * 16, (1024 * 1024) * 16);
            checkPartLength(((1024 * 1024) * 16) + 1, (1024 * 1024) * 16);
            for (let size = (1024 * 1024) * 16;
                size <= (1024 * 1024) * 512;
                size *= 2) {
                checkPartLength((size * 1000), size);
                // 512MB part sizes should allow for up to 10K parts.
                if (size === (1024 * 1024) * 512) {
                    checkPartLength((size * 1000) + 1, size);
                } else {
                    checkPartLength((size * 1000) + 1, size * 2);
                }
            }
            checkPartLength(MAX_AWS_OBJECT_SIZE, 1024 * 1024 * 1024);
        });
    });

    describe('::_getRanges', () => {
        it('should get a list of ranges with content length 0B', () => {
            const ranges = task._getRanges(0, false);
            assert.strictEqual(ranges.length, 1);
            assert.strictEqual(ranges[0], null);
        });

        it('should get a list of ranges with content length 1B', () => {
            const ranges = task._getRanges(1, false);
            assert.strictEqual(ranges.length, 1);
            const expected = { start: 0, end: 0 };
            assert.deepStrictEqual(ranges[0], expected);
        });

        it('should get a list of ranges with content length 5MB + 1B', () => {
            const ranges = task._getRanges(MIN_AWS_PART_SIZE, false);
            assert.strictEqual(ranges.length, 1);
            const expected = { start: 0, end: MIN_AWS_PART_SIZE - 1 };
            assert.deepStrictEqual(ranges[0], expected);
        });

        it('should get a list of ranges with content length 16MB', () => {
            const contentLength = (1024 * 1024) * 16; // 16MB
            const ranges = task._getRanges(contentLength, false);
            assert.strictEqual(ranges.length, 1);
            const expected = { start: 0, end: (1024 * 1024) * 16 - 1 };
            assert.deepStrictEqual(ranges[0], expected);
        });

        it('should get a list of ranges with content length 16MB + 1B', () => {
            const contentLength = ((1024 * 1024) * 16) + 1;
            const ranges = task._getRanges(contentLength, false);
            assert.strictEqual(ranges.length, 2);
            let expected = { start: 0, end: ((1024 * 1024) * 16) - 1 };
            assert.deepStrictEqual(ranges[0], expected);
            expected = { start: contentLength - 1, end: contentLength - 1 };
            assert.deepStrictEqual(ranges[ranges.length - 1], expected);
        });

        it('should get a list of ranges with content length of 16000MB', () => {
            const sixteenMB = (1024 * 1024) * 16;
            const contentLength = ((1024 * 1024) * 16) * 1000;
            const ranges = task._getRanges(contentLength, false);
            assert.strictEqual(ranges.length, 1000);
            let expected = { start: 0, end: sixteenMB - 1 };
            assert.deepStrictEqual(ranges[0], expected);
            expected = { start: sixteenMB, end: (sixteenMB * 2) - 1 };
            assert.deepStrictEqual(ranges[1], expected);
            expected = {
                start: sixteenMB * (ranges.length - 1),
                end: contentLength - 1,
            };
            assert.deepStrictEqual(ranges[ranges.length - 1], expected);
        });

        it('should get a list of ranges with content length 16000MB + 1B',
        () => {
            const contentLength = (((1024 * 1024) * 16) * 1000) + 1;
            const ranges = task._getRanges(contentLength, false);
            assert.strictEqual(ranges.length, 501);
            const thirtyTwoMB = (1024 * 1024) * 32;
            let expected = { start: 0, end: thirtyTwoMB - 1 };
            assert.deepStrictEqual(ranges[0], expected);
            expected = { start: thirtyTwoMB, end: (thirtyTwoMB * 2) - 1 };
            assert.deepStrictEqual(ranges[1], expected);
            expected = { start: contentLength - 1, end: contentLength - 1 };
            assert.deepStrictEqual(ranges[ranges.length - 1], expected);
        });

        it('should get a list of 10K ranges', () => {
            const fiveHundredTwelveMB = (1024 * 1024) * 512;
            const contentLength = fiveHundredTwelveMB * 10000;
            const ranges = task._getRanges(contentLength, false);
            assert.strictEqual(ranges.length, 10000);
            let expected = { start: 0, end: fiveHundredTwelveMB - 1 };
            assert.deepStrictEqual(ranges[0], expected);
            expected = {
                start: contentLength - fiveHundredTwelveMB,
                end: contentLength - 1,
            };
            assert.deepStrictEqual(ranges[ranges.length - 1], expected);
        });

        it('should not exceed a list of 10K ranges', () => {
            const oneGB = 1024 * 1024 * 1024;
            const contentLength = (((1024 * 1024) * 512) * 10000) + 1;
            const ranges = task._getRanges(contentLength, false);
            assert.strictEqual(ranges.length, 5001);
            let expected = { start: 0, end: oneGB - 1 };
            assert.deepStrictEqual(ranges[0], expected);
            expected = { start: contentLength - 1, end: contentLength - 1 };
            assert.deepStrictEqual(ranges[ranges.length - 1], expected);
        });

        it('should get a list of ranges with content length 5TB', () => {
            const contentLength = MAX_AWS_OBJECT_SIZE;
            const oneGB = 1024 * 1024 * 1024;
            const ranges = task._getRanges(contentLength, false);
            assert.strictEqual(ranges.length, 5120);
            let expected = { start: 0, end: oneGB - 1 };
            assert.deepStrictEqual(ranges[0], expected);
            expected = { start: contentLength - oneGB, end: contentLength - 1 };
            assert.deepStrictEqual(ranges[ranges.length - 1], expected);
        });

        it('should ensure all parts of the original object are intact',
        function test() {
            this.timeout(10000);
            const minMPUObjectSize = MIN_AWS_PART_SIZE + 1;
            const contentLengths = [MAX_AWS_OBJECT_SIZE];
            Array.from(Array(1024).keys()).forEach(n => {
                for (let i = minMPUObjectSize + n;
                    i <= MAX_AWS_OBJECT_SIZE;
                    i *= 2) {
                    contentLengths.push(i);
                }
            });
            contentLengths.forEach(contentLength => {
                const ranges = task._getRanges(contentLength, false);
                assert(ranges.length <= 10000);
                let sum = 0;
                for (let i = 0; i < ranges.length; i++) {
                    const { start, end } = ranges[i];
                    const rangeSize = end - start + 1; // Range is inclusive.
                    const isLastPart = i + 1 === ranges.length;
                    assert(rangeSize >= isLastPart ? 1 : MIN_AWS_PART_SIZE);
                    assert(rangeSize <= MAX_AWS_PART_SIZE);
                    if (!isLastPart) {
                        assert(rangeSize % 1024 === 0);
                    }
                    sum += rangeSize;
                }
                assert(sum === contentLength);
            });
        });

        it('should get single part count for GCP', () => {
            const contentLength = (1024 * 1024) * 5;
            const ranges = task._getRanges(contentLength, true);
            assert(ranges.length === 1);
        });

        it('should use GCP calculation for ranges exceeding 512MB * 1024',
        () => {
            const contentLength = ((1024 * 1024) * 512) * 1024;
            let ranges = task._getRanges(contentLength, true);
            assert(ranges.length === 1024);
            ranges = task._getRanges(contentLength + 1, true);
            assert(ranges.length === 513);
        });

        it('should get <= 1024 ranges for part count 1025-10000', () => {
            const partSize = 1024 * 1024 * 1024 + 1;
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
