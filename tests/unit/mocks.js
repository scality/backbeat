const assert = require('assert');
const { EventEmitter } = require('events');
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

class MockRequestAPI extends EventEmitter {
    /**
     * @param {object} args -
     * @param {object} response -
     * @param {object} response.error -
     * @param {object} response.res -
     */
    constructor(args, response) {
        super();
        this.response = response;
        this.args = args;
        this.doFn = null;
    }

    send(cb) {
        if (typeof this.doFn === 'function') {
            this.doFn(this.args);
        }

        cb(this.response.error, this.response.res);
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

    deleteObjectFromExpiration() {
        this.times.deleteObjectFromExpiration += 1;
        return new MockRequestAPI(this.response.error, this.response.data);
    }

    setResponse(error, data) {
        this.response = { error, data };
    }

    batchDelete(params, cb) {
        this.times.batchDeleteResponse += 1;

        const resp = this.batchDeleteResponse;
        const req = new MockRequestAPI(params, resp);

        if (typeof cb !== 'function') {
            return req;
        }

        return cb(resp.error, resp.res);
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

class S3RequestMock extends EventEmitter {
    constructor(error, data) {
        super();
        this.error = error;
        this.data = data;
    }

    send(cb) {
        cb(this.error, this.data);
    }
}

class S3ClientMock {
    constructor() {
        this.response = null;
        this.calls = {
            headObject: 0,
            deleteObject: 0,
            deleteMultipartObject: 0,
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

    headObject() {
        this.calls.headObject += 1;
        this.assertRespIsSet();
        return new S3RequestMock(this.response.error, this.response.data);
    }

    deleteObject() {
        this.calls.deleteObject += 1;
        this.assertRespIsSet();
        return new S3RequestMock(this.response.error, this.response.data);
    }

    deleteMultipartObject() {
        this.calls.deleteMultipartObject += 1;
        this.assertRespIsSet();
        return new S3RequestMock(this.response.error, this.response.data);
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
