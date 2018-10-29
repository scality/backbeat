'use strict'; // eslint-disable-line strict
const url = require('url');
const querystring = require('querystring');

/**
 * Class representing a Backbeat API Request
 *
 * @class
 */
class BackbeatRequest {
    /**
     * @constructor
     * @param {http.IncomingMessage} req - request object
     * @param {http.ServerResponse} res - response object
     * @param {Logger.newRequestLogger} log - logger object
     */
    constructor(req, res, log) {
        this._request = req;
        this._response = res;
        this._log = log;
        this._httpMethod = this._request.method;
        this._route = null;
        this._hasValidPrefix = null;
        this._statusCode = 0;
        this._error = null;
        this._routeDetails = {};
        // default to 'application/json' unless specified by parser
        this._contentType = 'application/json';

        // Use to store matched route from Arsenal list of backbeat routes
        this._matchedRoute = null;

        this.setRoute(this._request.url);
        this._parseRoute();
    }

    /**
     * Parse the route details for any of the crr routes (retry, pause, resume).
     * @param {Array} parts - The route schema split by '/'
     * @param {String} query - The query string.
     * @return {undefined}
     */
    _parseCRRRoutes(parts, query) {
        if (parts[1] && parts[1] === 'failed') {
            const { versionId, marker, sitename } = querystring.parse(query);
            this._routeDetails.extension = parts[0];
            this._routeDetails.status = parts[1];
            this._routeDetails.bucket = parts[2];
            this._routeDetails.key = parts.slice(3).join('/');
            this._routeDetails.versionId = versionId;
            this._routeDetails.marker = marker;
            this._routeDetails.sitename = sitename;
        } else {
            // for now: pause/resume/status
            this._routeDetails.extension = parts[0];
            this._routeDetails.status = parts[1];
            this._routeDetails.site = parts[2] || 'all';
            if (parts[3]) {
                this._routeDetails.schedule = parts[3] === 'schedule';
            }
        }
    }

    /**
     * Parse the route details for any of the ingestion routes (pause, resume)
     * @param {Array} parts - The route schema split by '/'
     * @return {undefined}
     */
    _parseIngestionRoutes(parts) {
        this._routeDetails.extension = parts[0];
        this._routeDetails.status = parts[1];
        this._routeDetails.site = parts[2] || 'all';
        if (parts[3]) {
            // not implemented. Save to route details and error will escalate
            // from parser
            this._routeDetails.schedule = parts[3] === 'schedule';
        }
    }

    /**
     * Parse the route details for any of the metrics routes
     * @param {Array} parts - route schema split by '/'
     * @param {String} query - The query string.
     * @return {undefined}
     */
    _parseMetricsRoutes(parts, query) {
        this._routeDetails.category = parts[0];
        this._routeDetails.extension = parts[1];
        this._routeDetails.site = parts[2];
        // optional field, default to 'all'
        this._routeDetails.type = parts[3] || 'all';
        this._routeDetails.bucketName = parts[4];
        this._routeDetails.objectKey = parts.slice(5).join('/');
        this._routeDetails.versionId = querystring.parse(query).versionId;
    }

    /**
     * Parse the route details for any of the monitoring routes
     * @param {Array} parts - route schema split by '/'
     * @return {undefined}
     */
    _parseMonitoringRoutes(parts) {
        this._routeDetails.category = parts[0];
        this._routeDetails.type = parts[1];
    }

    /**
     * Parse a route and store to this._routeDetails
     * A route will have certain a specific structure following:
     * /_/metrics/<extension>/<site>/<specific-metric>
     * All parts of the route are required except for <specific-metric>
     * @return {undefined}
     */
    _parseRoute() {
        const { pathname, query } = url.parse(this._route);
        const parts = pathname ? pathname.split('/') : [];

        // crr retry/pause/resume routes
        if (parts[0] === 'crr') {
            this._parseCRRRoutes(parts, query);
        } else if (parts[0] === 'metrics') {
            this._parseMetricsRoutes(parts, query);
        } else if (parts[0] === 'monitoring') {
            this._parseMonitoringRoutes(parts);
        } else if (parts[0] === 'ingestion') {
            this._parseIngestionRoutes(parts);
        }
        return;
    }

    /**
     * Get route details object
     * @return {object} this._routeDetails
     */
    getRouteDetails() {
        return this._routeDetails;
    }

    /**
     * Get the http request method
     * @return {string} http request method
     */
    getHTTPMethod() {
        return this._httpMethod;
    }

    /**
     * Get logger object
     * @return {object} Logger object
     */
    getLog() {
        return this._log;
    }

    /**
     * Set logger object
     * @param {object} log - new Logger object
     * @return {BackbeatRequest} itself
     */
    setLog(log) {
        this._log = log;
        return this;
    }

    /**
     * Get http request object
     * @return {object} Http request object
     */
    getRequest() {
        return this._request;
    }

    /**
     * Set http request object
     * @param {object} request - new Http request object
     * @return {BackbeatRequest} itself
     */
    setRequest(request) {
        this._request = request;
        return this;
    }

    /**
     * Get http response object
     * @return {object} Http response object
     */
    getResponse() {
        return this._response;
    }

    /**
     * Set http response object
     * @param {object} response - new Http response object
     * @return {BackbeatRequest} itself
     */
    setResponse(response) {
        this._response = response;
        return this;
    }

    /**
     * Get status code of request
     * @return {number} Http status code
     */
    getStatusCode() {
        return this._statusCode;
    }

    /**
     * Set status code of request
     * @param {number} code - new Http status code
     * @return {BackbeatRequest} itself
     */
    setStatusCode(code) {
        this._statusCode = code;
        return this;
    }

    /**
     * Get initial route prefix validity check
     * @return {boolean} true if request.url began with "/_/"
     */
    getHasValidPrefix() {
        return this._hasValidPrefix;
    }

    /**
     * Get route
     * @return {string} current route
     */
    getRoute() {
        return this._route;
    }

    /**
     * Set route
     * @param {string} route - route string
     * @return {BackbeatRequest} itself
     */
    setRoute(route) {
        this._hasValidPrefix = route.startsWith('/_/');
        if (this._hasValidPrefix) {
            this._route = route.substring(3);
        } else {
            this._route = route;
        }
        return this;
    }

    /**
     * Get the matched route from Arsenal list of backbeat routes
     * @return {Object} matched route object
     */
    getMatchedRoute() {
        return this._matchedRoute;
    }

    /**
     * Set the matched route from Arsenal list of backbeat routes
     * Extra properties may be added from BackbeatAPI.findValidRoute
     * @param {Object} route - matched route object
     * @return {BackbeatRequest} itself
     */
    setMatchedRoute(route) {
        this._matchedRoute = route;
        return this;
    }

    /**
     * Get the content-type of a given request
     * @return {string} content-type
     */
    getContentType() {
        return this._contentType;
    }

    /**
     * Set the content-type of a given request
     * Specific routes will require specific content types (i.e. promclient)
     * @param {string} type - content-type for this request
     * @return {BackbeatRequest} itself
     */
    setContentType(type) {
        this._contentType = type;
        return this;
    }

    /**
     * Status check to see if valid request
     * @return {boolean} valid status check
     */
    getStatus() {
        return ((this._statusCode >= 200 && this._statusCode < 300)
            && !this._error);
    }
}

module.exports = BackbeatRequest;
