/**
 * Increment a Prometheus counter metric
 * @typedef { (labels: MetricLabels, value?: number) => void } CounterInc
 */

/**
 * Set a Prometheus gauge metric
 * @typedef { (labels: MetricLabels, value: number) => void } GaugeSet
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
 * Wrap Gauge Set wraps a Prometheus Guage's set method
 *
 * @param {promClient.Gauge} metric - Prom gauge metric
 * @returns {GaugeSet} - Function used to set gauge
 */
function wrapGaugeSet(metric) {
    return (labels, value) => {
        metric.set(labels, value);
    };
}

module.exports = {
    wrapCounterInc,
    wrapGaugeSet,
}
