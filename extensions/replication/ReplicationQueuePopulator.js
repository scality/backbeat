
const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const QueueEntry = require('./utils/QueueEntry');

class ReplicationQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.repConfig = params.config;
    }

    filter(entry) {
        this.publish(this.repConfig.topic,
                     `${queueEntry.getBucket()}/${queueEntry.getObjectKey()}`,
                     JSON.stringify(entry));
    }
}

module.exports = ReplicationQueuePopulator;
