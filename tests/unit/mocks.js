const assert = require('assert');
const { ObjectMD } = require('arsenal').models;

class GarbageCollectorProducerMock {
    constructor() {
        this.receivedEntry = null;
    }

    publishActionEntry(gcEntry) {
        this.receivedEntry = gcEntry;
    }

    getReceivedEntry() {
        return this.receivedEntry;
    }
}

class BackbeatProducerMock {
    constructor() {
        this.receivedEntry = null;
        this.topic = null;
    }

    sendToTopic(coldGcTopic, gcEntries, cb) {
        this.receivedEntry = gcEntries;
        this.topic = coldGcTopic;
        cb();
    }

    getReceivedEntry() {
        return this.receivedEntry;
    }

    getReceivedTopic() {
        return this.topic;
    }
}

class BackbeatClientMock {
    constructor() {
        this.response = null;
        this.batchDeleteResponse = {};
        this.times = {
            batchDeleteResponse: 0,
            deleteObjectFromExpiration: 0,
        };
    }

    send(command) {
        const commandName = command.constructor.name;
        if (commandName === 'DeleteObjectFromExpirationCommand') {
            this.times.deleteObjectFromExpiration += 1;
            
            if (this.response?.error) {
                const err = this.response.error;
                if (err.statusCode && !err.$metadata) {
                    err.$metadata = { httpStatusCode: err.statusCode };
                }
                return Promise.reject(err);
            }
            return Promise.resolve(this.response?.data || {});
        }
        
        if (commandName === 'BatchDeleteCommand') {
            this.times.batchDeleteResponse += 1;
            
            const resp = this.batchDeleteResponse;
            if (resp.error) {
                return Promise.reject(resp.error);
            }
            return Promise.resolve(resp.res || {});
        }
        
        return Promise.resolve({});
    }

    setResponse(error, data) {
        this.response = { error, data };
    }
}

class BackbeatMetadataProxyMock {
    constructor() {
        this.mdObj = null;
        this.receivedMd = null;
        this.indexesObj = null;
        this.receivedIdxObj = null;
        this.error = null;
    }

    setMdObj(mdObj) {
        this.mdObj = mdObj;
    }

    getMetadata(params, log, cb) {
        if (this.error) {
            return cb(this.error);
        }
        return cb(null, { Body: this.mdObj.getSerialized() });
    }

    putMetadata(params, log, cb) {
        this.receivedMd = JSON.parse(params.mdBlob);
        this.mdObj = ObjectMD.createFromBlob(params.mdBlob).result;
        return cb();
    }

    getBucketIndexes(bucket, log, cb) {
        if (this.error) {
            return cb(this.error);
        }
        return cb(null, this.indexesObj);
    }

    setError(error) {
        this.error = error;
    }

    putBucketIndexes(bucket, indexes, log, cb) {
        if (this.error) {
            return cb(this.error);
        }
        this.receivedIdxObj = indexes;
        return cb();
    }

    getReceivedMd() {
        return this.receivedMd;
    }
}


class ProcessorMock {
    constructor(lcConfig, s3Client, backbeatClient, backbeatMetadataProxy, gcProducer, coldProducer, gcConfig, logger) {
        this.lcConfig = lcConfig;
        this.s3Client = s3Client;
        this.backbeatMetadataProxy = backbeatMetadataProxy;
        this.backbeatClient = backbeatClient;
        this.gcProducer = gcProducer;
        this.coldProducer = coldProducer;
        this._gcConfig = gcConfig;
        this.logger = logger;
    }

    getStateVars() {
        return {
            lcConfig: this.lcConfig,
            backbeatClient: this.backbeatMetadataProxy,
            gcProducer: this.gcProducer,
            coldProducer: this.coldProducer,
            gcConfig: this._gcConfig,
            logger: this.logger,
            getBackbeatClient: () => this.backbeatClient,
            getBackbeatMetadataProxy: () => this.backbeatMetadataProxy,
            getS3Client: () => this.s3Client,
        };
    }
}

class S3ClientMock {
    constructor() {
        this.response = null;
        this.calls = {
            headObject: 0,
            deleteObject: 0,
            deleteMultipartObject: 0,
            abortMultipartUpload: 0,
        };
    }

    setResponse(error, data) {
        this.response = { error, data };
    }

    unsetResponse() {
        this.response = null;
    }

    assertRespIsSet() {
        assert(typeof this.response === 'object');
    }

    send(command) {
        this.assertRespIsSet();
        
        const commandName = command.constructor.name;
        if (commandName === 'HeadObjectCommand') {
            this.calls.headObject += 1;
        } else if (commandName === 'DeleteObjectCommand') {
            this.calls.deleteObject += 1;
        } else if (commandName === 'AbortMultipartUploadCommand') {
            this.calls.abortMultipartUpload += 1;
        } else if (commandName === 'DeleteMultipartObjectCommand') {
            this.calls.deleteMultipartObject += 1;
        }
        
        const { error, data } = this.response;
        if (error) {
            if (error.statusCode && !error.$metadata) {
                error.$metadata = { httpStatusCode: error.statusCode };
            }
            return Promise.reject(error);
        }
        return Promise.resolve(data || {});
    }
}

module.exports = {
    ProcessorMock,
    GarbageCollectorProducerMock,
    BackbeatMetadataProxyMock,
    BackbeatClientMock,
    S3ClientMock,
    BackbeatProducerMock,
};
