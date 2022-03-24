'use strict'; // eslint-disable-line

const assert = require('assert');
const sinon = require('sinon');
const { Logger } = require('werelogs');
const ZookeeperMock = require('zookeeper-mock');
const IngestionReader = require('../../../lib/queuePopulator/IngestionReader');

class MockLogConsumer {
    constructor(params) {
        this.params = params || {};
    }
}

describe('IngestionReader', () => {
    let zkMock;

    beforeEach(() => {
        zkMock = new ZookeeperMock();
    });

    it('Should strip metadata v1 prefixes from object entries', done => {
        const mockExtension = {
            filter: sinon.spy(),
        };
        const ingestionReader = new IngestionReader({
            logId: 'test-log-reader',
            zkClient: zkMock.createClient('localhost:2181'),
            logConsumer: new MockLogConsumer(),
            logger: new Logger('test:logReaderWithExtension'),
            extensions: [mockExtension],
            bucketdConfig: {
                bucket: 'example-ingestion-bucket',
                name: 'example-bucket-to-ingest-to',
            },
            ingestionConfig: {
                zookeeperPath: 'example-zookeeper-path'
            }
        });
        const record = {
            db: 'example-ingestion-bucket',
        };
        const masterEntry = {
            type: 'example-type',
            key: '\x7fMexample-key',
            value: 'example-value'
        };
        const versionEntry = {
            type: 'example-type',
            key: '\x7fMexample-key',
            value: 'example-value'
        };
        ingestionReader._processLogEntry({}, record, masterEntry);
        ingestionReader._processLogEntry({}, record, versionEntry);
        const expectedArgs = {
            type: 'example-type',
            bucket: 'example-bucket-to-ingest-to',
            key: 'example-key',
            value: 'example-value',
        };
        assert(mockExtension.filter.firstCall.calledWith(expectedArgs));
        assert(mockExtension.filter.secondCall.calledWith(expectedArgs));
        done();
    });

    it('Should not change keys of objects in v0 format', done => {
        const mockExtension = {
            filter: sinon.spy(),
        };
        const ingestionReader = new IngestionReader({
            logId: 'test-log-reader',
            zkClient: zkMock.createClient('localhost:2181'),
            logConsumer: new MockLogConsumer(),
            logger: new Logger('test:logReaderWithExtension'),
            extensions: [mockExtension],
            bucketdConfig: {
                bucket: 'example-ingestion-bucket',
                name: 'example-bucket-to-ingest-to',
            },
            ingestionConfig: {
                zookeeperPath: 'example-zookeeper-path'
            }
        });
        const record = {
            db: 'example-ingestion-bucket',
        };
        const masterEntry = {
            type: 'example-type',
            key: 'fMexample-key',
            value: 'example-value'
        };
        const versionEntry = {
            type: 'example-type',
            key: 'fVexample-key',
            value: 'example-value'
        };
        ingestionReader._processLogEntry({}, record, masterEntry);
        ingestionReader._processLogEntry({}, record, versionEntry);
        const expectedArgs = {
            type: 'example-type',
            bucket: 'example-bucket-to-ingest-to',
            key: 'fMexample-key',
            value: 'example-value',
        };
        assert(mockExtension.filter.firstCall.calledWith(expectedArgs));
        expectedArgs.key = 'fVexample-key';
        assert(mockExtension.filter.secondCall.calledWith(expectedArgs));
        done();
    });

});
