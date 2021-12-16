const { ZenkoMetrics } = require('arsenal').metrics;

const ingestionObjectsQueued = ZenkoMetrics.createCounter({
    name: 'ingestion_objects_queued_total',
    help: 'Total number of Kafka messages produced by the ingestion populator',
    labelNames: ['origin', 'locationName', 'raftID'],
});

const ingestionJournalEntriesProcessed = ZenkoMetrics.createCounter({
    name: 'ingestion_journal_entries_processed_total',
    help: 'Total amount of journal entries processed by the ingestion populator',
    labelNames: ['origin', 'locationName', 'raftID'],
});

const ingestionLogReadOffset = ZenkoMetrics.createGauge({
    name: 'ingestion_read_offset',
    help: 'Current read offset of metadata journal',
    labelNames: ['origin', 'locationName', 'raftID'],
});

const ingestionLogSize = ZenkoMetrics.createGauge({
    name: 'metadata_journal_size',
    help: 'Current size of metadata journal',
    labelNames: ['origin', 'locationName', 'raftID'],
});

class IngestionPopulatorMetrics {
    static onIngestionJournalProcessed(recordsRead, locationName, raftID) {
        ingestionJournalEntriesProcessed.inc({
            origin: 'ingestion',
            locationName,
            raftID,
        }, recordsRead);
    }

    static onIngestionQueued(topicEntries, locationName, raftID) {
        ingestionObjectsQueued.inc({
            origin: 'ingestion',
            locationName,
            raftID,
        }, topicEntries.length);
    }

    static onIngestionLogSaved(logOffset, logSize, locationName, raftID) {
        ingestionLogReadOffset.set({
            origin: 'ingestion',
            locationName,
            raftID,
        }, logOffset);

        if (logSize) {
            ingestionLogSize.set({
                origin: 'ingestion',
                locationName,
                raftID,
            }, logSize);
        }
    }
}

module.exports = IngestionPopulatorMetrics;
