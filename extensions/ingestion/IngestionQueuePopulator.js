const async = require('async');

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');

class IngestionQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.config = params.config;
    }

    createZkPath(cb, source) {
        if (!source || !source.raftCount) {
            return cb();
        }
        const { zookeeperPath } = this.extConfig;
        const targetZenkoBucket = source.name;
        return async.times(source.raftCount, (index, next) => {
            const path = `${zookeeperPath}/${targetZenkoBucket}` +
                `/raft-id-dispatcher/provisions/${index}`;
            return this.zkClient.mkdirp(path, err => {
                if (err) {
                    if (err.name !== 'NO_NODE') {
                        this.log.error('error creating zookeeper path', {
                            method: 'IngestionQueuePopulator.createZkPath',
                            zookeeperPath, error: err,
                        });
                        return next(err);
                    }
                }
                return next();
            });
        }, cb);
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
