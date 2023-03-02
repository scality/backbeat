const { ZenkoMetrics } = require('arsenal').metrics;

const ingestionObjectsQueued = ZenkoMetrics.createCounter({
    name: 's3_ingestion_objects_queued_total',
    help: 'Total number of Kafka messages produced by the ingestion populator',
    labelNames: ['origin', 'locationNameRaftID'],
});

const ingestionJournalEntriesProcessed = ZenkoMetrics.createCounter({
    name: 's3_ingestion_journal_entries_processed_total',
    help: 'Total number of journal entries processed by the ingestion populator',
    labelNames: ['origin', 'locationNameRaftID'],
});

const ingestionLogReadOffset = ZenkoMetrics.createGauge({
    name: 's3_ingestion_read_offset',
    help: 'Current read offset of metadata journal',
    labelNames: ['origin', 'locationNameRaftID'],
});

const ingestionLogSize = ZenkoMetrics.createGauge({
    name: 's3_ingestion_metadata_journal_size',
    help: 'Current size of metadata journal',
    labelNames: ['origin', 'locationNameRaftID'],
});

const ingestionKafkaPublish = ZenkoMetrics.createCounter({
    name: 's3_ingestion_kafka_publish_status_total',
    help: 'Total number of operations by the ingestion populator kafka producer',
    labelNames: ['origin', 'status'],
});

const ingestionZookeeperOp = ZenkoMetrics.createCounter({
    name: 's3_ingestion_zookeeper_operations_total',
    help: 'Total number of zookeeper operations by the ingestion populator',
    labelNames: ['origin', 'op', 'status'],
});

const ingestionSourceOp = ZenkoMetrics.createCounter({
    name: 's3_ingestion_source_operations_total',
    help: 'Total number of source operations by the ingestion populator',
    labelNames: ['origin', 'op', 'status'],
});

class IngestionPopulatorMetrics {
    static onIngestionJournalProcessed(recordsRead, locationName, raftID) {
        ingestionJournalEntriesProcessed.inc({
            origin: 'ingestion',
            locationNameRaftID: `${locationName}-${raftID}`,
        }, recordsRead);
    }

    static onIngestionQueued(topicEntries, locationName, raftID) {
        ingestionObjectsQueued.inc({
            origin: 'ingestion',
            locationNameRaftID: `${locationName}-${raftID}`,
        }, topicEntries.length);
    }

    static onIngestionLogSaved(logOffset, logSize, locationName, raftID) {
        ingestionLogReadOffset.set({
            origin: 'ingestion',
            locationNameRaftID: `${locationName}-${raftID}`,
        }, logOffset);

        if (logSize) {
            ingestionLogSize.set({
                origin: 'ingestion',
                locationNameRaftID: `${locationName}-${raftID}`,
            }, logSize);
        }
    }

    static onIngestionKafkaPublish(status) {
        ingestionKafkaPublish.inc({
            origin: 'ingestion',
            status,
        });
    }

    static onZookeeperOp(op, status) {
        ingestionZookeeperOp.inc({
            origin: 'ingestion',
            op,
            status,
        });
    }

    static onIngestionSourceOp(op, status) {
        ingestionSourceOp.inc({
            origin: 'ingestion',
            op,
            status,
        });
    }
}

module.exports = IngestionPopulatorMetrics;
