const { isMasterKey } = require('arsenal/lib/versioning/Version');
const { usersBucket } = require('arsenal').constants;

const QueuePopulatorExtension =
    require('../../lib/queuePopulator/QueuePopulatorExtension');

class NotificationQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.notificationConfig = params.config;
    }

    filter(entry) {
        // ignore bucket op
        if (entry.bucket === usersBucket) {
            return undefined;
        }
        if (!isMasterKey(entry.key)) {
            return this._filterVersionedKey(entry);
        }
        if (!entry.bucket) {
            this.log.trace('skipping entry because missing bucket name');
            return undefined;
        }
        // TODO: do not publish object metadata or the entire entry, only send
        // the name of the object, bucket name and the event type
        // pre-filter the entries based on the filters set in the notification
        // configuration (get the config from zookeeper)
        this.publish(this.notificationConfig.topic, entry.bucket,
            JSON.stringify(entry));
        return undefined;
    }
}

module.exports = NotificationQueuePopulator;
