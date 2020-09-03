const assert = require('assert');

const messageUtil
    = require('../../../../extensions/notification/utils/message');
const notifConstants
    = require('../../../../extensions/notification/constants');

const { eventMessageProperty } = notifConstants;
const logEntryValue = {
    acl: {
        Canned: 'authenticated-read',
        FULL_CONTROL: [],
        WRITE: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    },
    key: '',
    location: [{
        key: '70FBD0849D7CEA7CC2759C384876725991169320',
        size: 11,
        start: 0,
        dataStoreName: 'us-east-1',
        dataStoreType: 'scality',
        dataStoreETag: '1:3749f52bb326ae96782b42dc0a97b4c1',
    }],
    isDeleteMarker: false,
    tags: {},
    replicationInfo: {
        status: '',
        backends: [],
        content: [],
        destination: '',
        storageClass: '',
        role: '',
        storageType: '',
        dataStoreVersionId: '',
    },
    dataStoreName: 'us-east-1',
    originOp: 's3:ObjectCreated:Put',
    'last-modified': '2020-08-30T05:27:08.075Z',
    'md-model-version': 3,
    versionId: '98401234771912999999RG001  7.14.2',
    'owner-display-name': 'owner',
    'owner-id':
        '9abe0ec063308bc0c7cd5f99e1260f942afb7e8682a86edc807795fa652ee28a',
    'content-length': 11,
    'content-type': 'text/plain',
    'content-md5': '3749f52bb326ae96782b42dc0a97b4c1',
    'x-amz-version-id': 'null',
    'x-amz-server-version-id': '',
    'x-amz-storage-class': 'STANDARD',
    'x-amz-server-side-encryption': '',
    'x-amz-server-side-encryption-aws-kms-key-id': '',
    'x-amz-server-side-encryption-customer-algorithm': '',
    'x-amz-website-redirect-location': '',
};

const testEntry = {
    bucket: 'bucket1',
    key: 'test.txt',
    eventType: 's3:ObjectCreated:Put',
    versionId: '98401234771912999999RG001  7.14.2',
    configurationId: 'config1',
};

describe('Notification message util', () => {
    describe('AddLogAttributes', () => {
        it('should add attributes to an notification entry', () => {
            const message
                = messageUtil.addLogAttributes(logEntryValue, testEntry);
            assert(message);
            for (const [key, value] of Object.entries(eventMessageProperty)) {
                assert.strictEqual(message[key], logEntryValue[value]);
            }
        });
        // event type for non-versioned object delete will not be available
        // in log, so it is set before adding attributes
        it('should not modify event type if log entry has different type',
            () => {
                const entry = Object.assign(testEntry, {
                    eventType: 's3:ObjectRemoved:Delete',
                });
                const message
                    = messageUtil.addLogAttributes(logEntryValue, entry);
                assert.strictEqual(message.eventType, entry.eventType);
            }
        );
    });
    describe('TransformToSpec', () => {
        it('should add transform the event source entry to spec', () => {
            const eventEntry
                = messageUtil.addLogAttributes(logEntryValue, testEntry);
            const message
                = messageUtil.transformToSpec(eventEntry);
            assert(message);
            assert(message.Records[0]);
            const record = message.Records[0];
            assert(record.requestParameters);
            assert(record.responseElements);
            assert(record.s3);
            const s3 = record.s3;
            assert(s3.bucket);
            assert(s3.object);
            assert.notStrictEqual(s3.object.versionId, eventEntry.versionId);
        });
    });
});
