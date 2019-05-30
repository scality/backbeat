const assert = require('assert');

const UpdateReplicationStatus =
    require('../../../extensions/replication/tasks/UpdateReplicationStatus');
const QueueEntry = require('../../../lib/models/QueueEntry');
const { replicationEntry } = require('../../utils/kafkaEntries');

const { Logger } = require('werelogs');
const log = new Logger('test:UpdateReplicationStatus');

function getCompletedEntry() {
    return QueueEntry.createFromKafkaEntry(replicationEntry)
        .toCompletedEntry('sf')
        .toCompletedEntry('replicationaws')
        .setSite('sf');
}

function getRefreshedEntry() {
    return QueueEntry.createFromKafkaEntry(replicationEntry).setSite('sf');
}

function checkReplicationInfo(site, status, updatedSourceEntry) {
    assert.strictEqual(
        updatedSourceEntry.getReplicationSiteStatus(site), status);
}

const rspMock = {
    getStateVars: () => ({
        repConfig: {
            replicationStatusProcessor: {},
        },
        sourceConfig: {
            auth: {},
            s3: {
                host: 'localhost',
                port: 8000,
            },
            transport: 'http',
        },
    }),
};

describe('UpdateReplicationStatus._getUpdatedSourceEntry()', () => {
    const updateReplicationStatus = new UpdateReplicationStatus(rspMock);

    it('should return a COMPLETED entry when site status is PENDING', () => {
        const sourceEntry = getCompletedEntry();
        const refreshedEntry = getRefreshedEntry();
        const updatedSourceEntry = updateReplicationStatus
              ._getUpdatedSourceEntry({ sourceEntry, refreshedEntry }, log);
        checkReplicationInfo('sf', 'COMPLETED', updatedSourceEntry);
        checkReplicationInfo('replicationaws', 'PENDING', updatedSourceEntry);
    });

    it('should return null when site status is COMPLETED and existing site ' +
    'status is COMPLETED', () => {
        const sourceEntry = getCompletedEntry();
        const refreshedEntry = getCompletedEntry();
        const updatedSourceEntry = updateReplicationStatus
              ._getUpdatedSourceEntry({ sourceEntry, refreshedEntry }, log);
        assert.strictEqual(updatedSourceEntry, null);
    });

    it('should return null when site status is FAILED and existing site ' +
    'status is COMPLETED', () => {
        const sourceEntry = getCompletedEntry()
              .toFailedEntry('sf')
              .setSite('sf');
        const refreshedEntry = getCompletedEntry();
        const updatedSourceEntry = updateReplicationStatus
              ._getUpdatedSourceEntry({ sourceEntry, refreshedEntry }, log);
        assert.strictEqual(updatedSourceEntry, null);
    });
});

describe('UpdateReplicationStatus._getNFSUpdatedSourceEntry()', () => {
    const updateReplicationStatus = new UpdateReplicationStatus(rspMock);

    it('should return a COMPLETED entry when metadata has not changed', () => {
        const sourceEntry = getCompletedEntry();
        const refreshedEntry = getRefreshedEntry();
        const updatedSourceEntry = updateReplicationStatus
            ._getNFSUpdatedSourceEntry(sourceEntry, refreshedEntry);
        checkReplicationInfo('sf', 'COMPLETED', updatedSourceEntry);
        checkReplicationInfo('replicationaws', 'PENDING', updatedSourceEntry);
    });

    it('should return a PENDING entry when MD5 mismatch', () => {
        const sourceEntry = getCompletedEntry();
        const refreshedEntry = getRefreshedEntry();
        refreshedEntry.setContentMd5('d41d8cd98f00b204e9800998ecf8427e');
        const updatedSourceEntry = updateReplicationStatus
            ._getNFSUpdatedSourceEntry(sourceEntry, refreshedEntry);
        checkReplicationInfo('sf', 'PENDING', updatedSourceEntry);
        checkReplicationInfo('replicationaws', 'PENDING', updatedSourceEntry);
    });

    it('should return a PENDING entry when tags mismatch', () => {
        const sourceEntry = getCompletedEntry();
        const refreshedEntry = getRefreshedEntry();
        refreshedEntry.setTags({ key: 'value' });
        const updatedSourceEntry = updateReplicationStatus
            ._getNFSUpdatedSourceEntry(sourceEntry, refreshedEntry);
        checkReplicationInfo('sf', 'PENDING', updatedSourceEntry);
        checkReplicationInfo('replicationaws', 'PENDING', updatedSourceEntry);
    });
});
