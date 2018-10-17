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
        this._route = req.url;
        this._statusCode = 0;
        this._error = null;
        this._routeDetails = {};

        this._parseRoute();
    }

    /**
     * Parse the route details for any of the retry routes.
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
     * Parse a route and store to this._routeDetails
     * A route will have certain a specific structure following:
     * /_/metrics/<extension>/<site>/<specific-metric>
     * All parts of the route are required except for <specific-metric>
     * @return {undefined}
     */
    _parseRoute() {
        // always drop first 3 chars. This is already validated in
        // BackbeatServer._isValidRequest
        const route = this._route.substring(3);
        const { pathname, query } = url.parse(route);
        // if healthcheck, just skip this
        const parts = pathname ? pathname.split('/') : [];
        if (parts[0] === 'crr') {
            this._parseRetryRoutes(parts, query);
            return;
        }
        if (parts.length < 3 || parts.length > 4) {
            // leave this._routeDetails undefined
            return;
        }
        this._routeDetails.category = parts[0];
        this._routeDetails.extension = parts[1];
        this._routeDetails.site = parts[2];
        if (parts.length === 4) {
            this._routeDetails.metric = parts[3];
        }
    }

    /**
     * Get route details object
     * @return {object} this._routeDetails
     */
    getRouteDetails() {
        return this._routeDetails;
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
     * Get route
     * @return {string} current route
     */
    getRoute() {
        return this._route;
    }

    /**
     * Set route
     * @param {string} route - new route string
     * @return {BackbeatRequest} itself
     */
    setRoute(route) {
        this._route = route;
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
