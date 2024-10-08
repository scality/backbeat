const assert = require('assert');
const async = require('async');
const sinon = require('sinon');

const QueueProcessor = require(
    '../../../extensions/notification/queueProcessor/QueueProcessor');

describe('notification QueueProcessor', () => {
    let qp;

    before(done => {
        qp = new QueueProcessor(null, null, null, {
            destinations: [
                {
                    resource: 'destId',
                    host: 'external-kafka-host',
                },
            ],
        }, 'destId', null);
        qp.bnConfigManager = {
            setConfig: () => {},
            getConfig: (bucket, cb) => cb(null, {
                bucket: 'mybucket',
                notificationConfiguration: {
                    queueConfig: [
                        {
                            queueArn: 'arn:scality:bucketnotif:::destId',
                            events: [
                                's3:ObjectCreated:*',
                            ],
                        },
                    ],
                },
            })
        };
        qp._setupDestination('kafka', done);
    });

    it('should send notification to external destination and invoke callback on delivery report', done => {
        const sendCb = sinon.stub().callsFake(cb => cb());
        const send = sinon.stub().callsFake((messages, cb) => setTimeout(() => sendCb(cb), 100));
        qp._destination._notificationProducer = {
            send,
        };
        qp.processKafkaEntry({
            value: JSON.stringify({
                bucket: 'mybucket',
                key: 'key',
                eventType: 's3:ObjectCreated:Put',
                value: '{}',
            }),
        }, err => {
            assert.ifError(err);

            assert(send.calledOnce);
            assert(sendCb.calledOnce);
            assert(Array.isArray(send.args[0][0]));
            assert(send.args[0][0][0].key === 'mybucket/key');
            assert(typeof send.args[0][0][0].message === 'string');
            const message = JSON.parse(send.args[0][0][0].message);
            assert(typeof message === 'object');
            assert(Array.isArray(message.Records));
            assert.strictEqual(message.Records.length, 1);
            assert.strictEqual(message.Records[0].eventName,
                's3:ObjectCreated:Put'
            );
            done();
        });
    });

    // FIXME: this test could fit better in Integration as it would
    // also test the resilience of the producer, but at the time this
    // test was written Bucket Notification tests are not yet running
    // in Integration (refs: BB-419, INTGR-895)
    it('should process 10K notifications with 1K concurrency to send to external destination',
    function test10KNotif(done) {
        this.timeout(5000);

        const origLogger = qp.logger;
        qp.logger = {
            debug: () => {},
            info: () => {},
        };
        // fake how Kafka Producer delivers pending delivery reports at a regular interval
        let pendingDeliveryReports = [];
        const flushDeliveryReportsInterval = setInterval(() => {
            const _pendingDeliveryReports = pendingDeliveryReports;
            pendingDeliveryReports = [];
            // eslint-disable-next-line no-restricted-syntax
            for (const deliveryReportCb of _pendingDeliveryReports) {
                deliveryReportCb();
            }
        }, 200);
        const send = sinon.stub().callsFake((messages, cb) => {
            // push the delivery report to be delivered asynchronously later
            pendingDeliveryReports.push(cb);
        });
        qp._destination._notificationProducer = {
            send,
        };
        async.timesLimit(10000, 1000, (i, iDone) => qp.processKafkaEntry({
            value: JSON.stringify({
                bucket: 'mybucket',
                key: `key-${i}`,
                eventType: 's3:ObjectCreated:Put',
                value: '{}',
            }),
        }, iDone), err => {
            assert.ifError(err);
            assert.strictEqual(pendingDeliveryReports.length, 0);
            qp.logger = origLogger;
            clearInterval(flushDeliveryReportsInterval);
            done();
        });
    });

    it('should send notification to external destination with delivery error', done => {
        const send = sinon.stub().callsFake((messages, cb) => cb(new Error('delivery error')));
        qp._destination._notificationProducer = {
            send,
        };
        qp.processKafkaEntry({
            value: JSON.stringify({
                bucket: 'mybucket',
                key: 'key',
                eventType: 's3:ObjectCreated:Put',
                value: '{}',
            }),
        }, err => {
            assert.ifError(err);

            assert(send.calledOnce);
            setTimeout(done, 100);
        });
    });

    it('should not send an entry without "eventType" attribute', done => {
        const send = sinon.spy();
        qp._destination._notificationProducer = {
            send,
        };
        qp.processKafkaEntry({
            value: JSON.stringify({
                bucket: 'mybucket',
                key: 'key',
                // no "eventType"
                value: '{}',
            }),
        }, err => {
            assert.ifError(err);
            // should not have been sent
            assert(send.notCalled);
            done();
        });
    });

    it('should not send an entry if the message is invalid JSON', done => {
        const send = sinon.spy();
        qp._destination._notificationProducer = {
            send,
        };
        qp.processKafkaEntry({
            value: 'notjson',
        }, err => {
            assert(err);
            // should not have been sent
            assert(send.notCalled);
            done();
        });
    });
});

describe('notification QueueProcessor with multiple rules', () => {
    let qp;
    let sendStub;

    before(done => {
        qp = new QueueProcessor(null, null, null, {
            destinations: [
                {
                    resource: 'destId',
                    host: 'external-kafka-host',
                },
            ],
        }, 'destId', null);

        qp.bnConfigManager = {
            setConfig: () => {},
            getConfig: (bucket, cb) => cb(null, {
                bucket: 'mybucket',
                notificationConfiguration: {
                    queueConfig: [
                        {
                            events: ['s3:ObjectCreated:*'],
                            queueArn: 'arn:scality:bucketnotif:::destId',
                            id: '0',
                            filterRules: [
                                {
                                    name: 'Prefix',
                                    value: 'toto/',
                                },
                            ],
                        }, {
                            events: ['s3:ObjectCreated:*'],
                            queueArn: 'arn:scality:bucketnotif:::destId',
                            id: '1',
                            filterRules: [
                                {
                                    name: 'Prefix',
                                    value: 'tata/',
                                },
                            ],
                        },
                    ],
                },
            }),
        };

        qp._setupDestination('kafka', done);
    });

    beforeEach(() => {
        sendStub = sinon.stub().callsFake((messages, cb) => setTimeout(() => cb(), 100));
        qp._destination._notificationProducer = {
            send: sendStub,
        };
    });

    it('should send notification to external destination if object matches the first rule', done => {
        qp.processKafkaEntry({
            value: JSON.stringify({
                dateTime: '2024-08-02T09:19:43.991Z',
                eventType: 's3:ObjectCreated:Put',
                region: 'us-east-1',
                schemaVersion: '3',
                size: '2',
                versionId: null,
                bucket: 'mybucket',
                key: 'toto/key',
            }),
        }, err => {
            assert.ifError(err);

            const [m] = sendStub.args[0][0];
            const { key, message } = m;
            assert.strictEqual(key, 'mybucket/toto/key');
            assert.deepStrictEqual(JSON.parse(message), {
                Records: [
                    {
                        eventVersion: '1.0',
                        eventSource: 'scality:s3',
                        eventTime: '2024-08-02T09:19:43.991Z',
                        awsRegion: 'us-east-1',
                        eventName: 's3:ObjectCreated:Put',
                        userIdentity: { principalId: null },
                        requestParameters: { sourceIPAddress: null },
                        responseElements: {
                            'x-amz-request-id': null,
                            'x-amz-id-2': null,
                        },
                        s3: {
                            s3SchemaVersion: '1.0',
                            configurationId: '0',
                            bucket: {
                                name: 'mybucket',
                                ownerIdentity: { principalId: null },
                                arn: null,
                            },
                            object: {
                                key: 'toto/key',
                                eTag: null,
                                size: '2',
                                versionId: null,
                                sequencer: null,
                            },
                        },
                    },
                ],
            });
            done();
        });
    });

    it('should send notification to external destination if object matches the second rule', done => {
        qp.processKafkaEntry({
            value: JSON.stringify({
                dateTime: '2024-08-02T09:19:43.991Z',
                eventType: 's3:ObjectCreated:Put',
                region: 'us-east-1',
                schemaVersion: '3',
                size: '2',
                versionId: null,
                bucket: 'mybucket',
                key: 'tata/key',
            }),
        }, err => {
            assert.ifError(err);

            const [m] = sendStub.args[0][0];
            const { key, message } = m;
            assert.strictEqual(key, 'mybucket/tata/key');
            assert.deepStrictEqual(JSON.parse(message), {
                Records: [
                    {
                        eventVersion: '1.0',
                        eventSource: 'scality:s3',
                        eventTime: '2024-08-02T09:19:43.991Z',
                        awsRegion: 'us-east-1',
                        eventName: 's3:ObjectCreated:Put',
                        userIdentity: { principalId: null },
                        requestParameters: { sourceIPAddress: null },
                        responseElements: {
                            'x-amz-request-id': null,
                            'x-amz-id-2': null,
                        },
                        s3: {
                            s3SchemaVersion: '1.0',
                            configurationId: '1',
                            bucket: {
                                name: 'mybucket',
                                ownerIdentity: { principalId: null },
                                arn: null,
                            },
                            object: {
                                key: 'tata/key',
                                eTag: null,
                                size: '2',
                                versionId: null,
                                sequencer: null,
                            },
                        },
                    },
                ],
            });
            done();
        });
    });

    it('should not send notification to external destination if object does not match any rule', done => {
        qp.processKafkaEntry({
            value: JSON.stringify({
                dateTime: '2024-08-02T09:19:43.991Z',
                eventType: 's3:ObjectCreated:Put',
                region: 'us-east-1',
                schemaVersion: '3',
                size: '2',
                versionId: null,
                bucket: 'mybucket',
                key: 'key.png',
            }),
        }, err => {
            assert.ifError(err);
            assert(sendStub.notCalled);

            done();
        });
    });
});

describe('notification QueueProcessor with one rule filtering by prefix and suffix', () => {
    let qp;
    let sendStub;

    before(done => {
        qp = new QueueProcessor(null, null, null, {
            destinations: [
                {
                    resource: 'destId',
                    host: 'external-kafka-host',
                },
            ],
        }, 'destId', null);

        qp.bnConfigManager = {
            setConfig: () => {},
            getConfig: (bucket, cb) => cb(null, {
                bucket: 'mybucket',
                notificationConfiguration: {
                    queueConfig: [
                        {
                            events: ['s3:ObjectCreated:*'],
                            queueArn: 'arn:scality:bucketnotif:::destId',
                            id: '0',
                            filterRules: [
                                {
                                    name: 'Prefix',
                                    value: 'toto/',
                                },
                                {
                                    name: 'Suffix',
                                    value: '.png',
                                },
                            ],
                        },
                    ],
                },
            }),
        };

        qp._setupDestination('kafka', done);
    });

    beforeEach(() => {
        sendStub = sinon.stub().callsFake((messages, cb) => setTimeout(() => cb(), 100));
        qp._destination._notificationProducer = {
            send: sendStub,
        };
    });

    it('should send notification to external destination if object matches all filter rules', done => {
        qp.processKafkaEntry({
            value: JSON.stringify({
                dateTime: '2024-08-02T09:19:43.991Z',
                eventType: 's3:ObjectCreated:Put',
                region: 'us-east-1',
                schemaVersion: '3',
                size: '2',
                versionId: null,
                bucket: 'mybucket',
                key: 'toto/key.png',
            }),
        }, err => {
            assert.ifError(err);

            assert(sendStub.calledOnce);
            const [m] = sendStub.args[0][0];
            const { key, message } = m;
            assert.strictEqual(key, 'mybucket/toto/key.png');
            assert.deepStrictEqual(JSON.parse(message), {
                Records: [
                    {
                        eventVersion: '1.0',
                        eventSource: 'scality:s3',
                        eventTime: '2024-08-02T09:19:43.991Z',
                        awsRegion: 'us-east-1',
                        eventName: 's3:ObjectCreated:Put',
                        userIdentity: { principalId: null },
                        requestParameters: { sourceIPAddress: null },
                        responseElements: {
                            'x-amz-request-id': null,
                            'x-amz-id-2': null,
                        },
                        s3: {
                            s3SchemaVersion: '1.0',
                            configurationId: '0',
                            bucket: {
                                name: 'mybucket',
                                ownerIdentity: { principalId: null },
                                arn: null,
                            },
                            object: {
                                key: 'toto/key.png',
                                eTag: null,
                                size: '2',
                                versionId: null,
                                sequencer: null,
                            },
                        },
                    },
                ],
            });
            done();
        });
    });

    it('should not send notification to external destination if object matches only the prefix filter rule', done => {
        qp.processKafkaEntry({
            value: JSON.stringify({
                dateTime: '2024-08-02T09:19:43.991Z',
                eventType: 's3:ObjectCreated:Put',
                region: 'us-east-1',
                schemaVersion: '3',
                size: '2',
                versionId: null,
                bucket: 'mybucket',
                key: 'toto/key',
            }),
        }, err => {
            assert.ifError(err);
            assert(sendStub.notCalled);

            done();
        });
    });

    it('should not send notification to external destination if object matches only the suffix filter rule', done => {
        qp.processKafkaEntry({
            value: JSON.stringify({
                dateTime: '2024-08-02T09:19:43.991Z',
                eventType: 's3:ObjectCreated:Put',
                region: 'us-east-1',
                schemaVersion: '3',
                size: '2',
                versionId: null,
                bucket: 'mybucket',
                key: 'key.png',
            }),
        }, err => {
            assert.ifError(err);
            assert(sendStub.notCalled);

            done();
        });
    });
});

describe('notification QueueProcessor with multiple rules and object matching all rules', () => {
    let qp;
    let sendStub;

    before(done => {
        qp = new QueueProcessor(null, null, null, {
            destinations: [
                {
                    resource: 'destId',
                    host: 'external-kafka-host',
                },
            ],
        }, 'destId', null);

        qp.bnConfigManager = {
            setConfig: () => {},
            getConfig: (bucket, cb) => cb(null, {
                bucket: 'mybucket',
                notificationConfiguration: {
                    queueConfig: [
                        {
                            events: ['s3:ObjectCreated:*'],
                            queueArn: 'arn:scality:bucketnotif:::destId',
                            id: '0',
                            filterRules: [
                                {
                                    name: 'Prefix',
                                    value: 'toto/',
                                },
                            ],
                        }, {
                            events: ['s3:ObjectCreated:*'],
                            queueArn: 'arn:scality:bucketnotif:::destId',
                            id: '1',
                            filterRules: [
                                {
                                    name: 'Suffix',
                                    value: '.png',
                                },
                            ],
                        },
                    ],
                },
            }),
        };

        qp._setupDestination('kafka', done);
    });

    beforeEach(() => {
        sendStub = sinon.stub().callsFake((messages, cb) => setTimeout(() => cb(), 100));
        qp._destination._notificationProducer = {
            send: sendStub,
        };
    });

    it('should send only one notification to external destination if object matches all rules', done => {
        qp.processKafkaEntry({
            value: JSON.stringify({
                dateTime: '2024-08-02T09:19:43.991Z',
                eventType: 's3:ObjectCreated:Put',
                region: 'us-east-1',
                schemaVersion: '3',
                size: '2',
                versionId: null,
                bucket: 'mybucket',
                key: 'toto/key.png',
            }),
        }, err => {
            assert.ifError(err);

            assert(sendStub.calledOnce);
            const [m] = sendStub.args[0][0];
            const { key, message } = m;
            assert.strictEqual(key, 'mybucket/toto/key.png');
            assert.deepStrictEqual(JSON.parse(message), {
                Records: [
                    {
                        eventVersion: '1.0',
                        eventSource: 'scality:s3',
                        eventTime: '2024-08-02T09:19:43.991Z',
                        awsRegion: 'us-east-1',
                        eventName: 's3:ObjectCreated:Put',
                        userIdentity: { principalId: null },
                        requestParameters: { sourceIPAddress: null },
                        responseElements: {
                            'x-amz-request-id': null,
                            'x-amz-id-2': null,
                        },
                        s3: {
                            s3SchemaVersion: '1.0',
                            configurationId: '0',
                            bucket: {
                                name: 'mybucket',
                                ownerIdentity: { principalId: null },
                                arn: null,
                            },
                            object: {
                                key: 'toto/key.png',
                                eTag: null,
                                size: '2',
                                versionId: null,
                                sequencer: null,
                            },
                        },
                    },
                ],
            });
            done();
        });
    });
});

describe('notification QueueProcessor destination id not matching the rule destination id', () => {
    let qp;
    let sendStub;

    before(done => {
        qp = new QueueProcessor(null, null, null, {
            destinations: [
                {
                    resource: 'destId',
                    host: 'external-kafka-host',
                },
            ],
        }, 'destId', null);

        qp._setupDestination('kafka', done);
    });

    beforeEach(() => {
        sendStub = sinon.stub().callsFake((messages, cb) => setTimeout(() => cb(), 100));
        qp._destination._notificationProducer = {
            send: sendStub,
        };
    });

    it('should not send notification if QueueProcessor destination id does not match the rule destination id', done => {
        const mismatchedARN = [
            'arn:scality:bucketnotif:::2destId',
            'arn:scality:bucketnotif:::destId2',
        ];

        mismatchedARN.forEach(arn => {
            qp.bnConfigManager = {
                setConfig: () => {},
                getConfig: (bucket, cb) => cb(null, {
                    bucket: 'mybucket',
                    notificationConfiguration: {
                        queueConfig: [
                            {
                                events: ['s3:ObjectCreated:*'],
                                queueArn: arn,
                                id: '0',
                                filterRules: [
                                    { name: 'Prefix', value: 'toto/' },
                                ],
                            },
                        ],
                    },
                }),
            };

            const kafkaEntry = {
                value: JSON.stringify({
                    dateTime: '2024-08-02T09:19:43.991Z',
                    eventType: 's3:ObjectCreated:Put',
                    region: 'us-east-1',
                    schemaVersion: '3',
                    size: '2',
                    versionId: null,
                    bucket: 'mybucket',
                    key: 'toto/key',
                }),
            };

            qp.processKafkaEntry(kafkaEntry, err => {
                assert.ifError(err);
                assert(sendStub.notCalled);
            });
        });

        done();
    });
});
