const { isMasterKey } = require('arsenal/lib/versioning/Version');

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');

class ReplicationQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.repConfig = params.config;
    }

    filter(entry) {
        if (entry.type !== 'put' || isMasterKey(entry.key)) {
            return;
        }
        const value = JSON.parse(entry.value);
        if (!value.replicationInfo ||
            value.replicationInfo.status !== 'PENDING') {
            return;
        }
        this.publish(this.repConfig.topic,
                     `${entry.bucket}/${entry.key}`,
                     JSON.stringify(entry));
    }
}

module.exports = ReplicationQueuePopulator;
