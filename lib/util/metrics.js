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
 * @param {LabelValues<string>} defaultLabels - Default labels to apply
 * @param {LabelValues<string>} labels - Labels to apply
 * @return {LabelValues<string>} Final list of labels to apply
 */
function mergeLabels(defaultLabels, labels) {
    return { ...(defaultLabels || {}), ...labels };
}

/**
 * WrapCounterInc wraps the Counters Inc method adding the metric labels used.
 *
 * @param {promClient.Counter} metric - Prom counter metric
 * @param {LabelValues<string>} defaultLabels - Default labels to apply
 * @returns {CounterInc} - Function used to increment counter
 */
function wrapCounterInc(metric, defaultLabels = {}) {
    return (labels, value) => {
        metric.inc(mergeLabels(defaultLabels, labels), value);
    };
}

/**
 * WrapGaugeSet wraps a Prometheus Guage's set method
 *
 * @param {promClient.Gauge} metric - Prom gauge metric
 * @param {LabelValues<string>} defaultLabels - Default labels to apply
 * @returns {GaugeSet} - Function used to set gauge
 */
function wrapGaugeSet(metric, defaultLabels = {}) {
    return (labels, value) => {
        metric.set(mergeLabels(defaultLabels, labels), value);
    };
}

/**
 * WrapHistogramObserve wraps a Prometheus Histogram's observe method
 *
 * @param {promClient.Histogram} metric - Prom histogram metric
 * @param {LabelValues<string>} defaultLabels - Default labels to apply
 * @returns {HistogramObserve} - Function used to observe histogram
 */
function wrapHistogramObserve(metric, defaultLabels = {}) {
    return (labels, value) => {
        metric.observe(mergeLabels(defaultLabels, labels), value);
    };
}

module.exports = {
    wrapCounterInc,
    wrapGaugeSet,
    wrapHistogramObserve,
};
