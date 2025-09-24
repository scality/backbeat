const AWS = require('aws-sdk');
const Service = require('aws-sdk').Service;


// for more info, see how S3 client is configured in aws-sdk
// (clients/s3.js and lib/services/s3.js)

AWS.apiLoader.services.backbeat = {};
Object.defineProperty(AWS.apiLoader.services.backbeat, '2017-07-01', {
    get: function get() {
        const model = require('./backbeat-2017-07-01.api.json');
        return model;
    },
    enumerable: true,
    configurable: true,
});
const BackbeatClient = Service.defineService('backbeat', ['2017-07-01']);

BackbeatClient.prototype.validateService = function validateService() {
    if (!this.config.region) {
        this.config.region = 'us-east-1';
    }
};

// Override setupRequestListeners to add custom extractError listener
BackbeatClient.prototype.setupRequestListeners = function setupRequestListeners(request) {
    request.addListener('extractError', this.extractError);
};

// Override default extractError to preserve HTTP response body for HTML responses
// Because S3C has an nginx proxy that can return HTML error responses
// For example a 400 Request Header Or Cookie Too Large
// That would be converted to UnknownError: BadRequest
BackbeatClient.prototype.extractError = function extractErrorHtml(resp) {
    const httpResponse = resp.httpResponse || {};
    const code = httpResponse.statusCode;
    const statusMessage = httpResponse.statusMessage;
    const body = httpResponse.body;
    const headers = httpResponse.headers || {};
    const contentType = (headers['content-type'] || '').toLowerCase();

    if (contentType.includes('text/html')) {
        const html = body && body.toString() || '';
        const title = html.match(/<title[^>]*>([^<]+)<\/title>/i);
        const message = title && title[1] || 'HTML error response';

        // eslint-disable-next-line no-param-reassign
        resp.error = AWS.util.error(new Error(), {
            code: `HTML ${statusMessage || 'Error'}`,
            message,
            statusCode: code,
            rawBody: html,
        });
    }
};

module.exports = BackbeatClient;
