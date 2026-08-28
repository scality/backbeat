const assert = require('assert');
const sinon = require('sinon');
const constants = require('../../../lib/constants');

const QueueProcessor =
    require('../../../extensions/replication/queueProcessor/QueueProcessor');

function getQueueProcessorConfig() {
    return [
        'backbeat-func-test-dummy-topic',
        { connectionString: '127.0.0.1:2181/backbeat',
          autoCreateNamespace: false },
        null,
        { hosts: 'localhost:9092' },
        { auth: { type: 'role',
            vault: { host: 'localhost:9093',
                port: 7777 } },
            s3: { host: 'localhost:9094',
                port: 7777 },
            transport: 'http',
        },
        {
            transport: 'http',
            auth: { type: 'role' },
            replicationEndpoint: {
                site: 'site-crr',
                servers: ['site-crr:8000'],
            },
        },
        { topic: 'backbeat-func-test-dummy-topic',
          replicationStatusTopic: 'backbeat-func-test-repstatus',
          queueProcessor: {
              retry: {
                  scality: { timeoutS: 5 },
                  azure: { timeoutS: 5 },
              },
              groupId: 'backbeat-func-test-group-id',
              mpuPartsConcurrency: 10,
          },
        },
        null,
        { topic: 'metrics-test-topic' },
        {},
        {},
        'site-crr',
    ];
}

describe('Queue Processor', () => {
    let qp;
    beforeEach(() => {
        qp = new QueueProcessor(...getQueueProcessorConfig());
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('handle liveness', () => {
        let mockLog;
        beforeEach(() => {
            mockLog = sinon.spy();
            mockLog.debug = sinon.spy();
        });

        afterEach(() => {
            sinon.restore();
        });

        it('with ready components', () => {
            const mockConsumer = {
                isReady: sinon.stub().returns(true),
            };
            const mockProducer = {
                isReady: sinon.stub().returns(true),
            };

            qp.replicationStatusProducer = mockProducer;
            qp._consumer = mockConsumer;
            const response = qp.handleLiveness(mockLog);
            assert.deepStrictEqual(response, []);
        });

        it('with undefined components', () => {
            const response = qp.handleLiveness(mockLog);
            assert.deepStrictEqual(
                response,
                [
                    {
                        component: 'Replication Status Producer',
                        status: constants.statusUndefined,
                        site: 'site-crr',
                    }, {
                        component: 'Consumer',
                        status: constants.statusUndefined,
                        site: 'site-crr',
                    },
                ]
            );
            // only need to test this once as it matches the response anyway
            sinon.assert.calledOnceWithExactly(
                mockLog.debug,
                sinon.match.any, // we don't care about the debug label
                {
                    replicationStatusProducer: constants.statusUndefined,
                    consumer: constants.statusUndefined,
                }
            );
        });

        it('with not ready components', () => {
            const mockConsumer = {
                isReady: sinon.stub().returns(false),
            };
            const mockProducer = {
                isReady: sinon.stub().returns(false),
            };

            qp.replicationStatusProducer = mockProducer;
            qp._consumer = mockConsumer;
            const response = qp.handleLiveness(mockLog);
            assert.deepStrictEqual(
                response,
                [
                    {
                        component: 'Replication Status Producer',
                        status: constants.statusNotReady,
                        site: 'site-crr',
                    }, {
                        component: 'Consumer',
                        status: constants.statusNotReady,
                        site: 'site-crr',
                    },
                ]
            );
        });
    });

    describe('processReplicationEntry fan-out', () => {
        function makeKafkaEntry(backends) {
            return {
                value: JSON.stringify({
                    bucket: 'src-bucket',
                    key: 'obj',
                    value: JSON.stringify({
                        'md-model-version': 2,
                        'replicationInfo': {
                            status: 'PENDING',
                            content: ['DATA', 'METADATA'],
                            destination: 'arn:aws:s3:::legacy-dest',
                            role: 'arn:aws:iam::111:role/src',
                            backends,
                        },
                    }),
                }),
            };
        }

        beforeEach(() => {
            qp.taskScheduler = { push: sinon.stub().callsArgWith(1, null) };
        });

        it('dispatches one task when a single backend matches this.site', async () => {
            const kafkaEntry = makeKafkaEntry([
                {
                    site: 'site-crr',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::222:role/dst',
                },
            ]);

            await qp.processReplicationEntry(kafkaEntry);
            sinon.assert.calledOnce(qp.taskScheduler.push);

            const pushed = qp.taskScheduler.push.firstCall.args[0];
            assert.strictEqual(pushed.entry.getDestination(), 'arn:aws:s3:::bucket-a');
            assert.strictEqual(pushed.entry.getRole(), 'arn:aws:iam::222:role/dst');
        });

        it('dispatches one task per backend when several share this.site', async () => {
            const kafkaEntry = makeKafkaEntry([
                {
                    site: 'site-crr',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::222:role/dst',
                },
                {
                    site: 'site-crr',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-b',
                    role: 'arn:aws:iam::222:role/dst',
                },
            ]);

            await qp.processReplicationEntry(kafkaEntry);
            sinon.assert.calledTwice(qp.taskScheduler.push);

            const destinations = qp.taskScheduler.push.getCalls().map(c => c.args[0].entry.getDestination()).sort();
            assert.deepStrictEqual(destinations, [
                'arn:aws:s3:::bucket-a',
                'arn:aws:s3:::bucket-b',
            ]);
        });

        it('skips when no PENDING backend matches this.site', async () => {
            const kafkaEntry = makeKafkaEntry([
                {
                    site: 'site-other',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-x',
                    role: 'arn:aws:iam::222:role/dst',
                },
            ]);

            await qp.processReplicationEntry(kafkaEntry);
            sinon.assert.notCalled(qp.taskScheduler.push);
        });

        it('skips PENDING backend that is for a different site', async () => {
            const kafkaEntry = makeKafkaEntry([
                {
                    site: 'site-crr',
                    status: 'COMPLETED',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::222:role/dst',
                },
                {
                    site: 'site-other',
                    status: 'PENDING',
                    dataStoreVersionId: '',
                    destination: 'arn:aws:s3:::bucket-b',
                    role: 'arn:aws:iam::222:role/dst',
                },
            ]);

            await qp.processReplicationEntry(kafkaEntry);
            sinon.assert.notCalled(qp.taskScheduler.push);
        });

        it('throws InternalError on a malformed kafka entry', async () => {
            const kafkaEntry = { value: 'not-valid-json{' };
            await assert.rejects(
                () => qp.processReplicationEntry(kafkaEntry),
                err => err.is.InternalError === true);
            sinon.assert.notCalled(qp.taskScheduler.push);
        });

        it('returns silently for canary heartbeat entries', async () => {
            // The populator's canary messages keep consumer groups warm:
            // deserialize, commit the offset, do nothing else.
            const kafkaEntry = { value: JSON.stringify({ canary: true }) };
            await qp.processReplicationEntry(kafkaEntry);
            sinon.assert.notCalled(qp.taskScheduler.push);
        });

        it('skips bucket entries when echo mode is disabled', async () => {
            qp.echoMode = false;
            // The QueueEntry parser recognises BucketQueueEntry when
            // `bucket` is the arsenal 'users..bucket' constant.
            const kafkaEntry = {
                value: JSON.stringify({
                    bucket: 'users..bucket',
                    key: 'some-canonical-id..|..src-bucket',
                    value: JSON.stringify({ creationDate: new Date().toJSON() }),
                }),
            };
            await qp.processReplicationEntry(kafkaEntry);
            sinon.assert.notCalled(qp.taskScheduler.push);
        });

        it('dispatches an EchoBucket task for bucket entries when echo mode is enabled', async () => {
            qp.echoMode = true;
            const kafkaEntry = {
                value: JSON.stringify({
                    bucket: 'users..bucket',
                    key: 'some-canonical-id..|..src-bucket',
                    value: JSON.stringify({ creationDate: new Date().toJSON() }),
                }),
            };
            await qp.processReplicationEntry(kafkaEntry);
            sinon.assert.calledOnce(qp.taskScheduler.push);
            const pushed = qp.taskScheduler.push.firstCall.args[0];
            assert.strictEqual(pushed.task.constructor.name, 'EchoBucket');
        });

        it('skips entries that are neither bucket nor object queue entries', async () => {
            // DeleteOpQueueEntry: neither Bucket nor Object → falls through
            // the routing without being pushed.
            const kafkaEntry = {
                value: JSON.stringify({
                    type: 'del',
                    bucket: 'src-bucket',
                    key: 'some-key',
                }),
            };
            await qp.processReplicationEntry(kafkaEntry);
            sinon.assert.notCalled(qp.taskScheduler.push);
        });
    });

    describe('processDataMoverEntry', () => {
        beforeEach(() => {
            qp.taskScheduler = { push: sinon.stub().callsArgWith(1, null) };
            qp.dataMoverTaskScheduler = {
                push: sinon.stub().callsArgWith(1, null),
            };
            qp._mProducer = { getProducer: () => null };
        });

        it('dispatches copyLocation actions to the data mover scheduler',
        done => {
            const kafkaEntry = {
                value: JSON.stringify({
                    action: 'copyLocation',
                    toLocation: 'site-crr',
                    target: {
                        bucket: 'src-bucket',
                        key: 'obj',
                        eTag: '"d41d8cd98f00b204e9800998ecf8427e"',
                    },
                }),
            };
            qp.processDataMoverEntry(kafkaEntry, () => {
                sinon.assert.calledOnce(qp.dataMoverTaskScheduler.push);
                sinon.assert.notCalled(qp.taskScheduler.push);
                done();
            });
        });
    });

    describe('constructor', () => {
        it('should use s3c site\'s host as a destination host', () => {
            const config = getQueueProcessorConfig();
            config[5].replicationEndpoint = {
                site: 'site-crr',
                servers: ['site-crr:8000'],
            };
            config[11] = 'site-crr';
            const qp = new QueueProcessor(...config);
            assert.deepStrictEqual(qp.destHosts.pickHost(), {
                host: 'site-crr',
                port: 8000,
            });
        });
        it('should setup echo mode when configured for site', () => {
            const config = getQueueProcessorConfig();
            config[5].replicationEndpoint = {
                site: 'site-crr',
                servers: ['site-crr:8000'],
                echo: true,
            };
            config[11] = 'site-crr';
            const setupEchoStub = sinon.stub(QueueProcessor.prototype, '_setupEcho').returns();
            const qp = new QueueProcessor(...config);
            assert.deepStrictEqual(qp.destHosts.pickHost(), {
                host: 'site-crr',
                port: 8000,
            });
            assert(setupEchoStub.calledOnce);
        });
        it('should not set destination host when using a non scality type site', () => {
            const config = getQueueProcessorConfig();
            config[5].replicationEndpoint = {
                site: 'site_aws',
                type: 's3_aws',
            };
            config[11] = 'site_aws';
            const qp = new QueueProcessor(...config);
            assert.strictEqual(qp.destHosts, null);
        });
    });
});
