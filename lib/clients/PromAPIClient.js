const http = require('http');
const querystring = require('querystring');

const { errors, jsutil } = require('arsenal');

class PromAPIClient {
    /**
     * @constructor
     * @param {string} promEndpoint - Prometheus API endpoint
     * @param {http.Agent} [promAPIHTTPAgent] - HTTP Agent
     */
    constructor(promEndpoint, promAPIHTTPAgent) {
        const [promHost, promPort] = promEndpoint.split(':');
        this._promHost = promHost;
        this._promPort = promPort;
        this._promAPIHTTPAgent = promAPIHTTPAgent;
    }

    /**
     * Convenience function to build a metric selector from a base
     * metric name and optional label names and label regexps
     *
     * @param {string} metricName - base name of metric
     * @param {object} [labels] - optional object to select a set of
     * labels by name, as { labelType: labelName, ... } or {
     * labelType: [labelName, labelName, ...], ... }
     * @param {object} [labelRegexps] - optional object to select a
     * set of labels by regular expression, as { labelType: regexp }
     * @return {string} a PROMQL metric name with label selectors set
     */
    static buildMetricSelector(metricName, labels, labelRegexps) {
        const selectors = [];
        if (labels) {
            Object.keys(labels).forEach(labelType => {
                const selector = labels[labelType];
                if (Array.isArray(selector)) {
                    // build a regexp to select multiple labels
                    selectors.push(`${labelType}=~"${selector.join('|')}"`);
                } else {
                    // select a simple label
                    selectors.push(`${labelType}="${selector}"`);
                }
            });
        }
        if (labelRegexps) {
            Object.keys(labelRegexps).forEach(labelType => {
                const selector = labelRegexps[labelType];
                selectors.push(`${labelType}=~"${selector}"`);
            });
        }
        return `${metricName}{${selectors.join(',')}}`;
    }

    /**
     * Send a PROMQL query to Prometheus API endpoint
     *
     * On success, get back the results as an array of values
     * (extracted from the response JSON body "data.results"
     * attribute).
     *
     * @param {object} params - query parameters
     * @param {string} params.query - PROMQL query
     * @param {werelogs.Logger} log - logger object
     * @param {function} cb - callback function: cb(err, results)
     * @return {undefined}
     */
    executeQuery(params, log, cb) {
        const cbOnce = jsutil.once(cb);
        const qs = querystring.stringify(params);
        const opts = {
            hostname: this._promHost,
            port: this._promPort,
            path: `/api/v1/query?${qs}`,
            agent: this._promAPIHTTPAgent,
        };
        log.debug('sending query to Prometheus API server', {
            method: 'PromAPIClient._executeQuery',
            host: opts.hostname,
            port: opts.port,
            params,
        });
        const begin = Date.now();
        const req = http.request(opts, res => {
            const buffers = [];
            res.on('data', chunk => {
                buffers.push(chunk);
            });
            return res.on('end', () => {
                const data = buffers.join('');
                log.debug(
                    'response received for query to Prometheus API server', {
                        method: 'PromAPIClient._executeQuery',
                        host: opts.hostname,
                        port: opts.port,
                        params,
                        payloadLength: data.length,
                        elapsedMs: Math.round(Date.now() - begin),
                    });
                let parsedResp;
                try {
                    parsedResp = JSON.parse(data);
                } catch (err) {
                    log.error(
                        'error parsing response from Prometheus API server', {
                            method: 'PromAPIClient._executeQuery',
                            host: opts.hostname,
                            port: opts.port,
                            path: opts.path,
                            error: err.message,
                            rawData: data.slice(0, 200),
                        });
                    return cbOnce(err);
                }
                if (res.statusCode !== 200 || parsedResp.status !== 'success') {
                    log.error(
                        'Prometheus API server returned an error status', {
                            method: 'PromAPIClient._executeQuery',
                            host: opts.hostname,
                            port: opts.port,
                            path: opts.path,
                            queryStatus: parsedResp.status,
                            errorType: parsedResp.errorType,
                            error: parsedResp.error,
                        });
                    if (res.statusCode !== 200) {
                        return cbOnce(errors.InternalError);
                    } else {
                        return cbOnce(errors.InternalError.customizeDescription(
                            parsedResp.error));
                    }
                }
                return cbOnce(null, parsedResp.data.result);
            });
        });
        req.end();
        req.on('error', err => {
            log.error('error querying Prometheus API server', {
                method: 'PromAPIClient._executeQuery',
                host: opts.hostname,
                port: opts.port,
                path: opts.path,
                error: err.message,
            });
            return cbOnce(err);
        });
    }
}

module.exports = PromAPIClient;
