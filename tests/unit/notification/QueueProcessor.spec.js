const assert = require('assert');
const sinon = require('sinon');

const QueueProcessor = require(
    '../../../extensions/notification/queueProcessor/QueueProcessor');

describe('notification QueueProcessor', () => {
    let qp;

    before(done => {
        qp = new QueueProcessor(null, null, null, {
            host: 'external-kafka-host',
        }, 'destId');
        qp.bnConfigManager = {
            getConfig: () => ({
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
            }),
            setConfig: () => {},
        };
        qp.bnConfigManager.setConfig('mybucket', {
            host: 'foo',
        });
        qp._setupDestination('kafka', done);
    });

    it('should send notification to external destination and invoke callback immediately', done => {
        const send = sinon.spy();
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
            assert(Array.isArray(send.args[0][0]));
            assert(send.args[0][0][0].key === 'mybucket/key');
            assert(typeof send.args[0][0][0].message === 'object');
            assert(Array.isArray(send.args[0][0][0].message.Records));
            assert.strictEqual(send.args[0][0][0].message.Records.length, 1);
            assert.strictEqual(
                send.args[0][0][0].message.Records[0].eventName,
                's3:ObjectCreated:Put'
            );
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
            assert(!send.calledOnce);
            done();
        });
    });
});
