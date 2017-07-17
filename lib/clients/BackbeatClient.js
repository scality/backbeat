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

module.exports = BackbeatClient;
