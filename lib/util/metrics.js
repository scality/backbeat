/**
 * Increment a Prometheus counter metric
 * @typedef { (labels: MetricLabels, value?: number) => void } CounterInc
 */

/**
 * Set a Prometheus gauge metric
 * @typedef { (labels: MetricLabels, value: number) => void } GaugeSet
 */

/**
 * Observe a Prometheus histogram metric
 * @typedef { (labels: MetricLabels, value: number) => void } HistogramObserve
 */

/**
 * WrapCounterInc wraps the Counters Inc method adding the metric labels used.
 *
 * @param {promClient.Counter} metric - Prom counter metric
 * @returns {CounterInc} - Function used to increment counter
 */
function wrapCounterInc(metric) {
    return (labels, value) => {
        metric.inc(labels, value);
    };
}

/**
 * WrapGaugeSet wraps a Prometheus Guage's set method
 *
 * @param {promClient.Gauge} metric - Prom gauge metric
 * @returns {GaugeSet} - Function used to set gauge
 */
function wrapGaugeSet(metric) {
    return (labels, value) => {
        metric.set(labels, value);
    };
}

/**
 * WrapHistogramObserve wraps a Prometheus Histogram's observe method
 *
 * @param {promClient.Histogram} metric - Prom histogram metric
 * @returns {HistogramObserve} - Function used to observe histogram
 */
function wrapHistogramObserve(metric) {
    return (labels, value) => {
        metric.observe(labels, value);
    };
}

module.exports = {
    wrapCounterInc,
    wrapGaugeSet,
    wrapHistogramObserve,
};
