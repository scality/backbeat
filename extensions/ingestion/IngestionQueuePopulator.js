const async = require('async');

const util = require('util');

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');

const LEADERS = '/leaders';
const OWNERS = '/owners';
const PROVISIONS = '/provisions';

// Temp testing
const RESET = '\x1b[0m';
// green
const COLORME = '\x1b[32m';

function logMe(str) {
    console.log(COLORME, str, RESET);
}

class IngestionQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.config = params.config;
    }

    // necessary for ProvisionDispatcher
    createZkPath(cb, source) {
        if (!source || !source.raftCount) {
            return cb();
        }
        const { zookeeperPath } = this.extConfig;
        const targetZenkoBucket = source.name;
        const basePath =
            `${zookeeperPath}/${targetZenkoBucket}/raft-id-dispatcher`;
        return async.parallel([
            next => {
                const path = `${basePath}${LEADERS}`;
                return this.zkClient.mkdirp(path, err => {
                    if (err) {
                        this.log.error('error creating zookeeper path', {
                            method: 'IngestionQueuePopulator.createZkPath',
                            zookeeperPath: path,
                            error: err,
                        });
                    }
                    return next(err);
                });
            },
            next => {
                const path = `${basePath}${OWNERS}`;
                return this.zkClient.mkdirp(path, err => {
                    if (err) {
                        this.log.error('error creating zookeeper path', {
                            method: 'IngestionQueuePopulator.createZkPath',
                            zookeeperPath: path,
                            error: err,
                        });
                    }
                    return next(err);
                });
            },
            next => async.times(source.raftCount, (index, done) => {
                const path = `${basePath}${PROVISIONS}/${index}`;
                return this.zkClient.mkdirp(path, err => {
                    if (err) {
                        if (err.name !== 'NO_NODE') {
                            this.log.error('error creating zookeeper path', {
                                method: 'IngestionQueuePopulator.createZkPath',
                                zookeeperPath, error: err,
                            });
                            return done(err);
                        }
                    }
                    return done();
                });
            }, next),
        ], cb);
    }

    // called by _processLogEntry in lib/queuePopulator/LogReader.js
    filter(entry) {
        if (entry.type !== 'put' && entry.type !== 'del') {
            logMe(`ENTRY TYPE IS NOT PUT OR DEL: ${entry.type}`)
            this.log.trace('skipping entry because not type put or del');
            return;
        }
        // Note that del entries at least have a bucket and key
        // and that bucket metadata entries at least have a bucket
        if (!entry.bucket) {
            logMe(`ENTRY BUCKET MISSING: ${JSON.stringify(entry)}`)
            this.log.trace('skipping entry because missing bucket name');
            return;
        }
        // logMe(`PUBLISH TO KAFKA!! ${this.config.topic} | ${entry.bucket}/${entry.key}`)
        // logMe(entry)
        // logMe(util.inspect(entry, { depth: 3 }));
        // let a = JSON.parse(entry.value);
        // logMe(Object.keys(a))
        // logMe(a.replicationInfo)
        // logMe(JSON.stringify(a.replicationInfo))
        // logMe(JSON.stringify(entry.value.tags))
        // logMe(JSON.stringify(entry));

        logMe('------------------')

        this.log.debug('publishing entry',
                       { entryBucket: entry.bucket, entryKey: entry.key });
        this.publish(this.config.topic,
                     `${entry.bucket}/${entry.key}`,
                     JSON.stringify(entry));
    }
}

module.exports = IngestionQueuePopulator;
