'use strict'; // eslint-disable-line strict

const http = require('http');

const { Clustering, errors, ipCheck } = require('arsenal');

const BackbeatRequest = require('./BackbeatRequest');
const BackbeatAPI = require('./BackbeatAPI');
const monitoringClient = require('../clients/monitoringHandler');

const WORKERS = 1;

class BackbeatServer {
    /**
     * @constructor
     * @param {Worker} [worker=null] - Track the worker using cluster
     * @param {object} config - configurations for server setup
     * @param {werelogs.Logger} logger - Logger object
     * @param {BackbeatAPI} backbeatAPI - BackbeatAPI instance
     */
    constructor(worker, config, logger, backbeatAPI) {
        this.server = null;
        this.worker = worker;
        this._config = config;
        this._port = config.port;
        this._logger = logger;
        this.backbeatAPI = backbeatAPI;
    }

    /**
     * Last log message to send per incoming request
     * @param {werelogs.newRequestLogger} logger - request logger
     * @param {object} req - request object
     * @param {object} res - response object
     * @param {object} desc - optional specific message from api
     * @return {undefined}
     */
    _logRequestEnd(logger, req, res, desc) {
        const info = {
            clientIp: req.socket.remoteAddress,
            clientPort: req.socket.remotePort,
            httpMethod: req.method,
            httpURL: req.url,
            httpCode: res.statusCode,
            httpMessage: res.statusMessage,
            description: desc && desc.description,
        };
        logger.end('finished handling request', info);
    }

    /**
     * Check if incoming request is valid
     * @param {object} req - incoming request object
     * @param {BackbeatRequest} backbeatRequest - Backbeat request object
     * @return {boolean} true if no errors
     */
    _isValidRequest(req, backbeatRequest) {
        const allowIp = ipCheck.ipMatchCidrList(
            this._config.healthChecks.allowFrom, req.socket.remoteAddress);
        if (!allowIp) {
            return this._errorResponse(errors.AccessDenied
                .customizeDescription('invalid origin ip request'),
                backbeatRequest);
        }

        const validMethods = ['GET', 'POST', 'DELETE'];
        if (!validMethods.includes(req.method)) {
            return this._errorResponse(errors.MethodNotAllowed,
                backbeatRequest);
        }

        if (!backbeatRequest.getHasValidPrefix()) {
            return this._errorResponse(errors.RouteNotFound
                .customizeDescription(`path ${backbeatRequest.getRoute()} does `
                    + 'not exist'), backbeatRequest);
        }

        const routeError = this.backbeatAPI.findValidRoute(backbeatRequest);
        if (routeError) {
            return this._errorResponse(routeError, backbeatRequest);
        }

        const queryError = this.backbeatAPI.validateQuery(backbeatRequest);
        if (queryError) {
            return this._errorResponse(queryError, backbeatRequest);
        }

        return true;
    }

    /**
     * Check if Kafka Producer and Zookeeper are working properly
     * @param {BackbeatRequest} backbeatRequest - Backbeat request object
     * @return {boolean} true if no errors
     */
    _areConditionsOk(backbeatRequest) {
        if (!this.backbeatAPI.isConnected()) {
            return this._errorResponse(errors.InternalError
                .customizeDescription('error connecting to internal client'),
                backbeatRequest);
        }

        return true;
    }

    /**
     * Server's error response handler
     * @param {arsenal.ArsenalError} err - arsenal error object
     * @param {BackbeatRequest} backbeatRequest - Backbeat request object
     * @return {undefined}
     */
    _errorResponse(err, backbeatRequest) {
        backbeatRequest.setStatusCode(err.code);
        this._response(err, backbeatRequest);
    }

    /**
     * Get the body of a POST request and route it accordingly.
     * @param {ClientRequest} req - The incoming request
     * @param {BackbeatRequest} bbRequest - The Backbeat API Request
     * @return {undefined}
     */
    _handlePOSTReq(req, bbRequest) {
        const data = [];
        const routeDetails = bbRequest.getMatchedRoute();

        req.on('data', chunk => data.push(chunk));
        req.on('end', () => {
            const { method } = routeDetails;
            const body = data.join('');
            return this.backbeatAPI[method](routeDetails, body, (err, data) => {
                if (err) {
                    return this._errorResponse(err, bbRequest);
                }
                return this._response(data, bbRequest);
            });
        });
        req.on('error', err => this._errorResponse(err, bbRequest));
        return;
    }

    /**
     * Server incoming request handler
     * @param {object} req - request object
     * @param {object} res - response object
     * @return {undefined}
     */
    _requestListener(req, res) {
        req.socket.setNoDelay();
        const bbRequest = new BackbeatRequest(req, res,
            this._logger.newRequestLogger());

        // check request conditions and all internal conditions here
        if (this._isValidRequest(req, bbRequest)
        && (this._areConditionsOk(bbRequest) ||
        bbRequest.getRoute().startsWith('healthcheck'))) {
            bbRequest.setStatusCode(200);

            if (bbRequest.getHTTPMethod() === 'POST') {
                return this._handlePOSTReq(req, bbRequest);
            }

            const routeDetails = bbRequest.getMatchedRoute();
            this.backbeatAPI[routeDetails.method](routeDetails, (err, data) => {
                if (err) {
                    this._errorResponse(err, bbRequest);
                } else {
                    this._response(data, bbRequest);
                }
            });
        }
        return undefined;
    }

    /**
     * Server's response to the client
     * @param {object} data - response to send to client
     * @param {BackbeatRequest} backbeatRequest - Backbeat request object
     * @return {object} res - response object
     */
    _response(data, backbeatRequest) {
        const log = backbeatRequest.getLog();
        const req = backbeatRequest.getRequest();
        const res = backbeatRequest.getResponse();
        const contentType = backbeatRequest.getContentType();
        const code = backbeatRequest.getStatusCode();
        log.trace('writing HTTP response', {
            method: 'BackbeatServer._response',
        });

        let payload;
        if (contentType === 'application/json') {
            payload = Buffer.from(JSON.stringify(data), 'utf8');
        } else {
            payload = data;
        }

        res.writeHead(code, {
            'Content-Type': contentType,
            'Content-Length': Buffer.byteLength(payload, 'utf8'),
        });
        this._logRequestEnd(log, req, res, data);
        return res.end(payload);
    }

    /**
     * start BackbeatServer
     * @return {undefined}
     */
    start() {
        this.server = http.createServer((req, res) => {
            this._requestListener(req, res);
        });

        this.server.on('listening', () => {
            const addr = this.server.address() || {
                address: '0.0.0.0',
                port: this._port,
            };
            this._logger.trace('server started', {
                address: addr.address,
                port: addr.port,
                pid: process.pid,
            });
        });
        this.server.listen(this._port);
    }

    /*
     * stop BackbeatServer and exit running process properly
     */
    stop() {
        this._logger.info(`worker ${this.worker} shutting down`);
        this.server.close();
        process.exit(0);
    }
}

/**
 * start the backbeat API server
 * @param {object} config - location of config file
 * @param {werelogs.Logger} Logger - Logger object
 * @param {function} cb - callback function
 * @return {undefined}
 */
function run(config, Logger) {
    const logger = new Logger('BackbeatServer');
    const apiLogger = new Logger('BackbeatAPI');
    const cluster = new Clustering(WORKERS, logger);

    // NOTE: internal timer is disabled until an alternative to an
    //  in-memory uptime is found
    const backbeatAPI = new BackbeatAPI(config, apiLogger,
        { timer: true });

    backbeatAPI.setupInternals(err => {
        if (err) {
            logger.error('internal error, please try again', {
                error: err,
            });
            process.exit(1);
        } else {
            cluster.start(worker => {
                const server = new BackbeatServer(worker, config.server, logger,
                    backbeatAPI);
                process.on('SIGINT', () => server.stop());
                server.start();
            });
            monitoringClient.collectDefaultMetrics({ timeout: 5000 });
        }
    });
}

module.exports = run;
