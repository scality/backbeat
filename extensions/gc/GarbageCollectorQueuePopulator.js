const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const ActionQueueEntry = require('../../lib/models/ActionQueueEntry');

class GarbageCollectorQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.gcConfig = params.config;
        this.metricsHandler = params.metricsHandler;
    }

    filter(entry) {
        // ZENKO-4179 TODO:
        // 1. This function would be asynchronous.
        //
        // 2. Get the deleted object metadata based on the entry's bucket and key
        // NOTE: for delete the latest version, we need to find a way to not send duplicated entries.
        // Francois Ferrand's team is fixing a similar "duplicated" issue, engage a discussion with them.
        //
        // 3. Use metadata to check the object's data location is cold.
        //
        // 4. Publish entry to GC topic with: object's archive info.

        const value = JSON.parse(entry.value);

        const actionEntry = ActionQueueEntry.create('deleteArchive')
            .addContext({
                origin: 'QueuePopulator-GC',
                ruleType: 'GC',
                bucketName: entry.bucket,
                objectKey: value.key,
                eTag: `"${value['content-md5']}"`
            })
            .setAttribute('serviceName', 'queue-populator-gc')
            .setAttribute('archiveInfo', {
                nsId: '',
                archiveId: '',
                archiveVersion: 1,
            });

        this.publish(
            this.gcConfig.topic,
            actionEntry.key,
            actionEntry.toKafkaMessage(),
        );
    }
}

module.exports = GarbageCollectorQueuePopulator;
