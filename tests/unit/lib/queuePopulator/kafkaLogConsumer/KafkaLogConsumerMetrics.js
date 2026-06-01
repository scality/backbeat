const assert = require('assert');
const sinon = require('sinon');
const { ZenkoMetrics } = require('arsenal').metrics;
const KafkaLogConsumerMetrics =
    require('../../../../../lib/queuePopulator/KafkaLogConsumer/KafkaLogConsumerMetrics');

describe('KafkaLogConsumerMetrics', () => {
    const log = { error: sinon.stub() };

    afterEach(() => {
        sinon.restore();
        log.error.resetHistory();
    });

    describe('onMissingKey', () => {
        it('should increment the s3_oplog_event_missing_key_total counter', () => {
            const metric = ZenkoMetrics.getMetric('s3_oplog_event_missing_key_total');
            const incStub = sinon.stub(metric, 'inc');
            KafkaLogConsumerMetrics.onMissingKey(log, 'update');
            assert(incStub.calledOnceWith({ opType: 'update' }));
            assert(log.error.notCalled);
        });

        it('should label as "unknown" when opType is missing', () => {
            const metric = ZenkoMetrics.getMetric('s3_oplog_event_missing_key_total');
            const incStub = sinon.stub(metric, 'inc');
            KafkaLogConsumerMetrics.onMissingKey(log, undefined);
            assert(incStub.calledOnceWith({ opType: 'unknown' }));
        });

        it('should swallow + log errors from inc', () => {
            const metric = ZenkoMetrics.getMetric('s3_oplog_event_missing_key_total');
            sinon.stub(metric, 'inc').throws(new Error('boom'));
            assert.doesNotThrow(() => KafkaLogConsumerMetrics.onMissingKey(log, 'insert'));
            assert(log.error.calledOnce);
            assert(log.error.calledWithMatch('failed to update prometheus metrics', {
                method: 'KafkaLogConsumerMetrics.onMissingKey',
            }));
        });
    });
});
