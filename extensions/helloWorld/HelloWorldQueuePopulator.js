const { usersBucket } = require('arsenal').constants;

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const ObjectQueueEntry = require('../../lib/models/ObjectQueueEntry');

// Only look for this key for this extension
const HELLOWORLD_KEY = 'helloworld';

class HelloWorldQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.hwConfig = params.config;
    }

    filter(entry) {
        // Start filtering out any metadata logs that don't comply with our
        // new hello world extension

        // we require an object key name in our entry
        if (entry.key === undefined) {
            return;
        }
        // ignore the internally used `usersBucket` entries. These are used
        // internally in Zenko for specific bucket use-cases
        if (entry.bucket === usersBucket) {
            return;
        }
        // ignore any entries that are not 'put' types
        if (entry.type !== 'put') {
            return;
        }

        const value = JSON.parse(entry.value);
        const queueEntry = new ObjectQueueEntry(entry.bucket, entry.key, value);
        const sanityCheckRes = queueEntry.checkSanity();
        if (sanityCheckRes) {
            return;
        }

        // ignore any keys that don't match the key we are looking for
        if (queueEntry.getObjectKey().toLowerCase() !== HELLOWORLD_KEY) {
            return;
        }

        this.log.trace('publishing a helloworld entry', {
            entry: queueEntry.getLogInfo(),
        });

        // cache entries and we will eventually send these entries to kafka
        // in `LogReader._processPublishEntries`
        this.publish(this.hwConfig.topic,
                     `${queueEntry.getBucket()}/${queueEntry.getObjectKey()}`,
                     JSON.stringify(entry));
    }
}

module.exports = HelloWorldQueuePopulator;
