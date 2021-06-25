const sinon = require('sinon');
const { wrapCounterInc, wrapGaugeSet } =
    require('../../../../lib/util/metrics');

describe.only('Metrics', () => {
    it('can wrap counter inc', () => {
        const mockCounter = sinon.spy();
        mockCounter.inc = sinon.spy();
        const incFn = wrapCounterInc(mockCounter);
        incFn('label', 1);
        sinon.assert.calledOnceWithExactly(mockCounter.inc, 'label', 1);
    });

    it('can wrap gauge set', () => {
        const mockGauge = sinon.spy();
        mockGauge.set = sinon.spy();
        const setFn = wrapGaugeSet(mockGauge);
        setFn('label', 15);
        sinon.assert.calledOnceWithExactly(mockGauge.set, 'label', 15);
    });
});
