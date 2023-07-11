const assert = require('assert');
const async = require('async');
const sinon = require('sinon');

const QueueProcessor = require(
    '../../../extensions/notification/queueProcessor/QueueProcessor');

describe('notification QueueProcessor', () => {
    let qp;

    before(done => {
        qp = new QueueProcessor(null, null, {
            destinations: [
                {
                    resource: 'destId',
                    host: 'external-kafka-host',
                },
            ],
        }, 'destId', null);
        qp._getConfig = (bucket, cb) => cb(null, {
            queueConfig: [
                {
                    queueArn: 'arn:scality:bucketnotif:::destId',
                    events: [
                        's3:ObjectCreated:*',
                    ],
                },
            ],
        });
        qp.bnConfigManager = {
            setConfig: () => {},
        };
        qp.bnConfigManager.setConfig('mybucket', {
            host: 'foo',
        });
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
