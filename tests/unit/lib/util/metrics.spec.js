const sinon = require('sinon');
const { wrapCounterInc, wrapGaugeSet, wrapHistogramObserve } =
    require('../../../../lib/util/metrics');

describe('Metrics', () => {
    it('can wrap counter inc', () => {
        const mockCounter = sinon.spy();
        mockCounter.inc = sinon.spy();
        const incFn = wrapCounterInc(mockCounter);
        incFn({ label: 'value' }, 1);
        sinon.assert.calledOnceWithExactly(mockCounter.inc, { label: 'value' }, 1);
    });

    it('can add default labels to wrapped counter', () => {
        const mockCounter = sinon.spy();
        mockCounter.inc = sinon.spy();
        const incFn = wrapCounterInc(mockCounter, { defaultLabel: 'default' });
        incFn({ label: 'value' }, 1);
        sinon.assert.calledOnceWithExactly(mockCounter.inc,
            { defaultLabel: 'default', label: 'value' }, 1);
    });

    it('can override default labels on wrapped counter', () => {
        const mockCounter = sinon.spy();
        mockCounter.inc = sinon.spy();
        const incFn = wrapCounterInc(mockCounter, { label: 'default' });
        incFn({ label: 'value' }, 1);
        sinon.assert.calledOnceWithExactly(mockCounter.inc, { label: 'value' }, 1);
    });

    it('can wrap gauge set', () => {
        const mockGauge = sinon.spy();
        mockGauge.set = sinon.spy();
        const setFn = wrapGaugeSet(mockGauge);
        setFn({ label: 'value' }, 15);
        sinon.assert.calledOnceWithExactly(mockGauge.set, { label: 'value' }, 15);
    });

    it('can add default labels to wrapped gauge', () => {
        const mockGauge = sinon.spy();
        mockGauge.set = sinon.spy();
        const setFn = wrapGaugeSet(mockGauge, { defaultLabel: 'default' });
        setFn({ label: 'value' }, 15);
        sinon.assert.calledOnceWithExactly(mockGauge.set,
            { defaultLabel: 'default', label: 'value' }, 15);
    });

    it('can override default labels to wrapped gauge', () => {
        const mockGauge = sinon.spy();
        mockGauge.set = sinon.spy();
        const setFn = wrapGaugeSet(mockGauge, { label: 'default' });
        setFn({ label: 'value' }, 15);
        sinon.assert.calledOnceWithExactly(mockGauge.set, { label: 'value' }, 15);
    });

    it('can wrap histogram observe', () => {
        const mockHistogram = sinon.spy();
        mockHistogram.observe = sinon.spy();
        const observeFn = wrapHistogramObserve(mockHistogram);
        observeFn({ label: 'value' }, 15);
        sinon.assert.calledOnceWithExactly(mockHistogram.observe, { label: 'value' }, 15);
    });

    it('can add default labels to wrapped histogram', () => {
        const mockHistogram = sinon.spy();
        mockHistogram.observe = sinon.spy();
        const observeFn = wrapHistogramObserve(mockHistogram,
            { defaultLabel: 'default' });
        observeFn({ label: 'value' }, 15);
        sinon.assert.calledOnceWithExactly(mockHistogram.observe,
            { defaultLabel: 'default', label: 'value' }, 15);
    });

    it('can add default labels to wrapped histogram', () => {
        const mockHistogram = sinon.spy();
        mockHistogram.observe = sinon.spy();
        const observeFn = wrapHistogramObserve(mockHistogram, { label: 'default' });
        observeFn({ label: 'value' }, 15);
        sinon.assert.calledOnceWithExactly(mockHistogram.observe, { label: 'value' }, 15);
    });
});
