const { ZenkoMetrics } = require('arsenal').metrics;

const ingestionObjectsQueued = ZenkoMetrics.createCounter({
    name: 'ingestion_objects_queued_total',
    help: 'Total number of Kafka messages produced by the ingestion populator',
    labelNames: ['origin'],
});

const ingestionJournalEntriesProcessed = ZenkoMetrics.createCounter({
    name: 'ingestion_journal_entries_processed_total',
    help: 'Total amount of journal entries processed by the ingestion populator',
    labelNames: ['origin'],
});

const ingestionLogReadOffset = ZenkoMetrics.createGauge({
    name: 'replication_read_offset',
    help: 'Current read offset of metadata journal',
    labelNames: ['origin'],
});

const ingestionLogSize = ZenkoMetrics.createGauge({
    name: 'replication_log_size',
    help: 'Current size of metadata journal',
    labelNames: ['origin'],
});

class IngestionPopulatorMetrics {
    static onIngestionJournalProcessed() {
        ingestionJournalEntriesProcessed.inc({
            origin: 'ingestion',
        });
    }

    static onIngestionQueued(topicEntries) {
        ingestionObjectsQueued.inc({
            origin: 'ingestion',
        }, topicEntries.length);
    }

    static onIngestionLogSaved(logOffset, logSize) {
        ingestionLogReadOffset.set({
            origin: 'ingestion',
        }, logOffset);

        if (logSize) {
            ingestionLogSize.set({
                origin: 'ingestion',
            }, logSize);
        }
    }
}

module.exports = IngestionPopulatorMetrics;
