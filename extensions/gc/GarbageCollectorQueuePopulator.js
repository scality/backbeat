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
        if (entry.key === undefined || entry.value === undefined) {
            // bucket updates have no key in raft log
            return;
        }

        if (entry.type !== 'put') {
            this.log.trace('skipping entry because not type put');
            return;
        }

        const value = JSON.parse(entry.value);

        // just handle delete markers for now, isPHD entries are another option
        if (!value.isDeleteMarker) {
            return;
        }

        // debug log, will remove -Ronnie
        console.log('filter:', entry);

        const actionEntry = ActionQueueEntry.create('deleteData')
            .addContext({
                origin: 'QueuePopulator-GC',
                ruleType: 'GC',
                bucketName: entry.bucket,
                objectKey: value.key,
                eTag: `"${value['content-md5']}"`
            })
            .setAttribute('source', entry.getAttribute('source'))
            .setAttribute('serviceName', 'queue-populator-gc')
            .setAttribute('target.accountId', 'not sure where this can come from')
            .setAttribute('target.owner', value['owner-id'])
            .setAttribute('target.locations', value.location);

        this.publish(
            this.gcConfig.topic,
            actionEntry.key,
            actionEntry.toKafkaMessage(),
        );
    }
}

module.exports = GarbageCollectorQueuePopulator;
