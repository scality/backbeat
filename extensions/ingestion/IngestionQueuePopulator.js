const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');

class IngestionQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.config = params.config;
    }

    // called by _processLogEntry in lib/queuePopulator/LogReader.js
    filter(entry) {
        if (entry.type !== 'put' && entry.type !== 'del') {
            this.log.trace('skipping entry because not type put or del');
            return;
        }
        // Note that del entries at least have a bucket and key
        // and that bucket metadata entries at least have a bucket
        if (!entry.bucket) {
            this.log.trace('skipping entry because missing bucket name');
            return;
        }
        this.log.debug('publishing entry',
                       { entryBucket: entry.bucket, entryKey: entry.key });
        this.publish(this.config.topic,
                     `${entry.bucket}/${entry.key}`,
                     JSON.stringify(entry));
    }
}

module.exports = IngestionQueuePopulator;
