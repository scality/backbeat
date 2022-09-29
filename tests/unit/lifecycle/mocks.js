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

class BackbeatClientMock {
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
    constructor(s3Client, backbeatClient, gcProducer, logger) {
        this.s3Client = s3Client;
        this.backbeatClient = backbeatClient;
        this.gcProducer = gcProducer;
        this.logger = logger;
    }

    getStateVars() {
        return {
            backbeatClient: this.backbeatClient,
            gcProducer: this.gcProducer,
            logger: this.logger,
            getBackbeatClient: () => this.backbeatClient,
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
    LifecycleObjectProcessorMock,
    GarbageCollectorProducerMock,
    BackbeatClientMock,
    S3ClientMock,
};
