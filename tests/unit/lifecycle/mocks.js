const assert = require('assert');
const { EventEmitter } = require('events');

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

class MockRequestAPI {
    /**
     * @param {object} args -
     * @param {object} response -
     * @param {object} response.error -
     * @param {object} response.res -
     */
    constructor(args, response) {
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
        this.batchDeleteResponse = {};
        this.times = {
            batchDeleteResponse: 0,
        };
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
    }

    setMdObj(mdObj) {
        this.mdObj = mdObj;
    }

    getMetadata(params, log, cb) {
        return cb(null, { Body: this.mdObj.getSerialized() });
    }

    putMetadata(params, log, cb) {
        this.receivedMd = JSON.parse(params.mdBlob);
        return cb();
    }

    getReceivedMd() {
        return this.receivedMd;
    }
}

class LifecycleObjectProcessorMock {
    constructor(s3Client, backbeatClient, backbeatMetadataProxy, gcProducer, logger) {
        this.s3Client = s3Client;
        this.backbeatMetadataProxy = backbeatMetadataProxy;
        this.backbeatClient = backbeatClient;
        this.gcProducer = gcProducer;
        this.logger = logger;
    }

    getStateVars() {
        return {
            backbeatClient: this.backbeatMetadataProxy,
            gcProducer: this.gcProducer,
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
        this.assertRespIsSet();
        return new S3RequestMock(this.response.error, this.response.data);
    }

    deleteObject() {
        this.assertRespIsSet();
        return new S3RequestMock(this.response.error, this.response.data);
    }

    deleteMultipartObject() {
        this.assertRespIsSet();
        return new S3RequestMock(this.response.error, this.response.data);
    }
}

module.exports = {
    LifecycleObjectProcessorMock,
    GarbageCollectorProducerMock,
    BackbeatMetadataProxyMock,
    BackbeatClientMock,
    S3ClientMock,
};
