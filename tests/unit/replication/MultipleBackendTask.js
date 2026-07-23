const assert = require('assert');
const jsutil = require('arsenal').jsutil;
const sinon = require('sinon');

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
        const error = { retryable };
        task.backbeatSource = {
            send: sinon.stub().rejects(error),
        };

        task._getAndPutMultipartUpload(sourceEntry, fakeLogger, err => {
            assert(err);
            // in case of retryable error, this shall be ignored
            // thanks to jsutil.once(), where the non-retryable test
            // expects an error.
            return done(err);
        });
    }

    beforeEach(() => {
        task = new MultipleBackendTask({
            getStateVars: () => ({
                repConfig: {
                    queueProcessor: {
                        retry: retryConfig,
                        mpuPartsConcurrency: 2,
                    },
                },
                sourceConfig: config.extensions.replication.source,
                destConfig: {
                    ...config.extensions.replication.destination,
                    replicationEndpoint: config.extensions.replication.destination.bootstrapList
                        .find(e => e.site === 'test-site-2'),
                },
                site: 'test-site-2',
                notificationConfigManager: {
                    getConfig: () => null
                },
                logger: fakeLogger,
                metricsHandler: {
                    rpo: () => {},
                }
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

    describe('::_setupRolesOnce', () => {
        const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');

        function makeEntry() {
            return new ObjectQueueEntry('source-bucket', 'key', {
                'md-model-version': 2,
                'replicationInfo': {
                    status: 'PENDING',
                    content: ['DATA', 'METADATA'],
                    destination: '',
                    role: 'arn:aws:iam::111:role/src',
                    backends: [{
                        site: 'test-site-2',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    }],
                },
            }).setSite('test-site-2');
        }

        it('matches V2 rules by Filter.Prefix', () => {
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/src',
                        Rules: [{
                            Status: 'Enabled',
                            Filter: { Prefix: '' },
                            Destination: {
                                Bucket: 'arn:aws:s3:::bucket-a',
                                StorageClass: 'test-site-2',
                            },
                        }],
                    },
                }),
            };

            return task._setupRolesOnce(makeEntry(), fakeLogger);
        });

        it('rejects with PreconditionFailed when no rule matches the object key', () => {
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/src',
                        Rules: [{
                            Status: 'Enabled',
                            Filter: { Prefix: 'logs/' },
                            Destination: {
                                Bucket: 'arn:aws:s3:::bucket-a',
                                StorageClass: 'test-site-2',
                            },
                        }],
                    },
                }),
            };

            return assert.rejects(
                task._setupRolesOnce(makeEntry(), fakeLogger),
                err => err.is.PreconditionFailed === true);
        });

        it('accepts V1 rules with top-level Prefix', () => {
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/src',
                        Rules: [{
                            Status: 'Enabled',
                            Prefix: '',
                            Destination: {
                                Bucket: 'arn:aws:s3:::bucket-a',
                                StorageClass: 'test-site-2',
                            },
                        }],
                    },
                }),
            };

            return task._setupRolesOnce(makeEntry(), fakeLogger);
        });
    });

    describe('::initiateMultipartUpload', () => {
        it('should use exponential backoff if retryable error', done => {
            const doneOnce = jsutil.once(done);
            setTimeout(() => {
                // inhibits further retries
                task.retryParams.timeoutS = 0;
                doneOnce();
            }, 4000); // Retries will exceed test timeout.
            requestInitiateMPU({ retryable: true }, err => {
                assert(err);
            });
        });

        it('should not use exponential backoff if non-retryable error', done => {
            requestInitiateMPU({ retryable: false }, err => {
                assert(err);
                done();
            });
        });
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

    describe('::processQueueEntry', () => {
        let queueEntry;
        beforeEach(() => {
            fakeLogger.newRequestLogger = () => fakeLogger;
            queueEntry = QueueEntry.createFromKafkaEntry(replicationEntry);
            sinon.stub(task, '_setupClients').yields(null);
            sinon.stub(task, '_refreshSourceEntry').resolves(queueEntry);
            sinon.stub(task, '_handleReplicationOutcome').callsFake(
                err => (err ? Promise.reject(err) : Promise.resolve(null)));
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should call delete marker handler function', done => {
            sinon.stub(queueEntry, 'getIsDeleteMarker').returns(true);
            const deleteMarkerHandler = sinon.stub(task, '_putDeleteMarker').yields(null);
            task.processQueueEntry(queueEntry, replicationEntry, err => {
                assert.ifError(err);
                assert(deleteMarkerHandler.calledOnce);
                return done();
            });
        });

        it('should skip entry when it has a COMPLETED state', done => {
            sinon.stub(queueEntry, 'getIsDeleteMarker').returns(false);
            sinon.stub(queueEntry, 'getReplicationContent').returns(['METADATA', 'DATA']);
            sinon.stub(queueEntry, 'getReplicationSiteStatus').returns('COMPLETED');
            task.processQueueEntry(queueEntry, replicationEntry, err => {
                assert(err.is.InvalidObjectState);
                return done();
            });
        });

        it('should call _putObjectTagging if tags were added and object was previously replicated', done => {
            sinon.stub(queueEntry, 'getIsDeleteMarker').returns(false);
            sinon.stub(queueEntry, 'getReplicationContent').returns(['METADATA', 'DATA', 'PUT_TAGGING']);
            sinon.stub(queueEntry, 'getReplicationSiteStatus').returns('PENDING');
            sinon.stub(queueEntry, 'getReplicationSiteDataStoreVersionId').returns('1234');
            const putTaggingHandler = sinon.stub(task, '_putObjectTagging').yields(null);
            task.processQueueEntry(queueEntry, replicationEntry, err => {
                assert.ifError(err);
                assert(putTaggingHandler.calledOnce);
                return done();
            });
        });

        it('should replicate whole object if putting tags and object wasn\'t previously replicated', done => {
            sinon.stub(queueEntry, 'getIsDeleteMarker').returns(false);
            sinon.stub(queueEntry, 'getReplicationContent').returns(['METADATA', 'DATA', 'PUT_TAGGING']);
            sinon.stub(queueEntry, 'getReplicationSiteStatus').returns('PENDING');
            sinon.stub(queueEntry, 'getReplicationSiteDataStoreVersionId').returns(null);
            task.repConfig.queueProcessor.minMPUSizeMB = 10;
            sinon.stub(queueEntry, 'getContentLength').returns(1000000);
            sinon.stub(queueEntry, 'isMultipartUpload').returns(false);
            const putObjectHandler = sinon.stub(task, '_getAndPutObject').yields(null);
            task.processQueueEntry(queueEntry, fakeLogger, err => {
                assert.ifError(err);
                assert(putObjectHandler.calledOnce);
                delete task.repConfig.queueProcessor.minMPUSizeMB;
                return done();
            });
        });

        it('should call _deleteObjectTagging if tags were removed  and object was previously replicated', done => {
            sinon.stub(queueEntry, 'getIsDeleteMarker').returns(false);
            sinon.stub(queueEntry, 'getReplicationContent').returns(['METADATA', 'DATA', 'DELETE_TAGGING']);
            sinon.stub(queueEntry, 'getReplicationSiteStatus').returns('PENDING');
            sinon.stub(queueEntry, 'getReplicationSiteDataStoreVersionId').returns('1234');
            const deleteTaggingHandler = sinon.stub(task, '_deleteObjectTagging').yields(null);
            task.processQueueEntry(queueEntry, replicationEntry, err => {
                assert.ifError(err);
                assert(deleteTaggingHandler.calledOnce);
                return done();
            });
        });

        it('should replicate whole object if deleting tags and object wasn\'t previously replicated', done => {
            sinon.stub(queueEntry, 'getIsDeleteMarker').returns(false);
            sinon.stub(queueEntry, 'getReplicationContent').returns(['METADATA', 'DATA', 'DELETE_TAGGING']);
            sinon.stub(queueEntry, 'getReplicationSiteStatus').returns('PENDING');
            sinon.stub(queueEntry, 'getReplicationSiteDataStoreVersionId').returns(null);
            task.repConfig.queueProcessor.minMPUSizeMB = 10;
            sinon.stub(queueEntry, 'getContentLength').returns(1000000);
            sinon.stub(queueEntry, 'isMultipartUpload').returns(false);
            const putObjectHandler = sinon.stub(task, '_getAndPutObject').yields(null);
            task.processQueueEntry(queueEntry, fakeLogger, err => {
                assert.ifError(err);
                assert(putObjectHandler.calledOnce);
                delete task.repConfig.queueProcessor.minMPUSizeMB;
                return done();
            });
        });

        it('should call MPU handler when object is bigger than threshold', done => {
            sinon.stub(queueEntry, 'getIsDeleteMarker').returns(false);
            sinon.stub(queueEntry, 'getReplicationContent').returns(['METADATA', 'DATA']);
            sinon.stub(queueEntry, 'getReplicationSiteStatus').returns('PENDING');
            task.repConfig.queueProcessor.minMPUSizeMB = 10;
            sinon.stub(queueEntry, 'getContentLength').returns(100000000);
            const mpuHandler = sinon.stub(task, '_getAndPutMultipartUpload').yields(null);
            task.processQueueEntry(queueEntry, replicationEntry, err => {
                assert.ifError(err);
                assert(mpuHandler.calledOnce);
                return done();
            });
        });

        it('should call MPU handler when object is tagged as an MPU', done => {
            sinon.stub(queueEntry, 'getIsDeleteMarker').returns(false);
            sinon.stub(queueEntry, 'getReplicationContent').returns(['METADATA', 'DATA']);
            sinon.stub(queueEntry, 'getReplicationSiteStatus').returns('PENDING');
            task.repConfig.queueProcessor.minMPUSizeMB = 10;
            sinon.stub(queueEntry, 'getContentLength').returns(1000000);
            sinon.stub(queueEntry, 'isMultipartUpload').returns(true);
            const mpuHandler = sinon.stub(task, '_getAndPutMultipartUpload').yields(null);
            task.processQueueEntry(queueEntry, fakeLogger, err => {
                assert.ifError(err);
                assert(mpuHandler.calledOnce);
                delete task.repConfig.queueProcessor.minMPUSizeMB;
                return done();
            });
        });

        it('should call normal put handler when object is not MPU', done => {
            sinon.stub(queueEntry, 'getIsDeleteMarker').returns(false);
            sinon.stub(queueEntry, 'getReplicationContent').returns(['METADATA', 'DATA']);
            sinon.stub(queueEntry, 'getReplicationSiteStatus').returns('PENDING');
            task.repConfig.queueProcessor.minMPUSizeMB = 10;
            sinon.stub(queueEntry, 'getContentLength').returns(1000000);
            sinon.stub(queueEntry, 'isMultipartUpload').returns(false);
            const putObjectHandler = sinon.stub(task, '_getAndPutObject').yields(null);
            task.processQueueEntry(queueEntry, fakeLogger, err => {
                assert.ifError(err);
                assert(putObjectHandler.calledOnce);
                delete task.repConfig.queueProcessor.minMPUSizeMB;
                return done();
            });
        });
    });

    describe('_initiateMPU', () => {
        it('should init mpu when location type is not azure', done => {
            task.backbeatSource = {
                send: sinon.stub().resolves({}),
            };
            task.destConfig = {
                replicationEndpoint: {
                    site: 'test-site',
                    type: 'aws_s3',
                },
            };
            task._initiateMPU(sourceEntry, fakeLogger, err => {
                assert.ifError(err);
                assert(task.backbeatSource.send.calledOnce);
                done();
            });
        });
        it('should not init mpu when location type is azure', done => {
            task.backbeatSource = {
                send: sinon.stub().resolves({}),
            };
            task.destConfig = {
                replicationEndpoint: {
                    site: 'test-site',
                    type: 'azure',
                },
            };
            task._initiateMPU(sourceEntry, fakeLogger, err => {
                assert.ifError(err);
                assert(task.backbeatSource.send.notCalled);
                done();
            });
        });
    });

    describe('per-backend key routing', () => {
        const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');

        function makeEntryWithTwoSameSiteBackends() {
            const entry = new ObjectQueueEntry('source-bucket', 'key', {
                'md-model-version': 2,
                'replicationInfo': {
                    status: 'PENDING',
                    content: ['DATA', 'METADATA'],
                    destination: '',
                    role: 'arn:aws:iam::111:role/src',
                    backends: [
                        {
                            site: 'test-site-2',
                            destination: 'arn:aws:s3:::bucket-a',
                            role: 'arn:aws:iam::aaa:role/r',
                            status: 'PENDING',
                            dataStoreVersionId: '',
                        },
                        {
                            site: 'test-site-2',
                            destination: 'arn:aws:s3:::bucket-b',
                            role: 'arn:aws:iam::bbb:role/r',
                            status: 'PENDING',
                            dataStoreVersionId: '',
                        },
                    ],
                },
                'tags': {},
            });
            entry.setReplicationBackend({
                site: 'test-site-2',
                destination: 'arn:aws:s3:::bucket-b',
                role: 'arn:aws:iam::bbb:role/r',
            });
            return entry;
        }

        it('_putObjectTaggingOnce writes the new versionId on the targeted backend only', done => {
            const entry = makeEntryWithTwoSameSiteBackends();
            task.backbeatSource = { send: sinon.stub().resolves({ versionId: 'new-vid' }) };
            sinon.stub(task, '_publishMetadataWriteMetrics');

            task._putObjectTaggingOnce(entry, fakeLogger, err => {
                assert.ifError(err);
                const backends = entry.getReplicationBackends();
                assert.strictEqual(backends[0].dataStoreVersionId, '');
                assert.strictEqual(backends[1].dataStoreVersionId, 'new-vid');
                done();
            });
        });

        it('_deleteObjectTaggingOnce writes the new versionId on the targeted backend only', done => {
            const entry = makeEntryWithTwoSameSiteBackends();
            task.backbeatSource = { send: sinon.stub().resolves({ versionId: 'del-vid' }) };
            sinon.stub(task, '_publishMetadataWriteMetrics');

            task._deleteObjectTaggingOnce(entry, fakeLogger, err => {
                assert.ifError(err);
                const backends = entry.getReplicationBackends();
                assert.strictEqual(backends[0].dataStoreVersionId, '');
                assert.strictEqual(backends[1].dataStoreVersionId, 'del-vid');
                done();
            });
        });
    });

    describe('_completeRangedMPU', () => {
        it('should abort MPU on part upload error', done => {            
            sinon.stub(task, '_getRanges').returns([
                { start: 0, end: 100 },
                { start: 101, end: 199 }
            ]);
            
            const putRangeFunc = sinon.stub(task, '_getRangeAndPutMPUPart');
            putRangeFunc.onCall(0).yields(null, {
                partNumber: 0,
                ETag: 'etag1',
            });
            putRangeFunc.onCall(1).yields(new Error('Upload failed'));
            
            const abortMpuFunc = sinon.stub(task, '_multipleBackendAbortMPU').yields();
            const completeMpuFunc = sinon.stub(task, '_completeMPU').yields();
            
            task.destConfig = {
                replicationEndpoint: {
                    site: 'test-site',
                    type: 'aws_s3',
                },
            };
            
            const uploadId = 'test-upload-id';
            task._completeRangedMPU(sourceEntry, uploadId, fakeLogger, err => {
                assert(err);
                assert.strictEqual(err.message, 'Upload failed');
                assert(abortMpuFunc.calledOnce);
                assert(completeMpuFunc.notCalled);
                done();
            });
        });
            
        it('should handle Azure special case for MPU parts', done => {
            sinon.stub(task, '_getRanges').returns([
                { start: 0, end: 100 },
                { start: 101, end: 199 }
            ]);
            
            const putRangeFunc = sinon.stub(task, '_getRangeAndPutMPUPart');
            putRangeFunc.onCall(0).yields(null, {
                partNumber: 0,
                ETag: 'etag1',
                numberSubParts: 2
            });
            putRangeFunc.onCall(1).yields(null, {
                partNumber: 1,
                ETag: 'etag2',
                numberSubParts: 1
            });
            
            const completeMpuFunc = sinon.stub(task, '_completeMPU').yields();
            
            task.destConfig = {
                replicationEndpoint: {
                    site: 'test-site',
                    type: 'azure',
                },
            };
            
            task._completeRangedMPU(sourceEntry, 'test-upload-id', fakeLogger, err => {
                assert.ifError(err);
                assert(completeMpuFunc.calledOnce);
                const completionData = completeMpuFunc.firstCall.args[2];
                assert(completionData[0].NumberSubParts);
                assert(completionData[1].NumberSubParts);
                done();
            });
        });
    });
});
