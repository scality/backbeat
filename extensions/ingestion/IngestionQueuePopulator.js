const async = require('async');
const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const IngestionPopulator = require('../../lib/queuePopulator/IngestionPopulator');

const zookeeper = require('../../lib/clients/zookeeper');
class IngestionQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        console.log('PARAMS', params);
        this.params = params;
        this.config = params.config;
        this.ingestionPopulator = [];
    }

    setIngestionPopulator(cb, source) {
        console.log('SOURCE', source);
        for (let i = 1; i <= source.raftCount; i++) {
            this.ingestionPopulator.push(new IngestionPopulator(this.params, i, source));
        }
        // this.ingestionPopulator = new IngestionPopulator(this.params, source);
    }

    createZkPath(cb, source) {
        const { zookeeperPath } = this.extConfig;
        const pathArray = [];
        if (source.raftCount) {
            for (let i = 1; i <= source.raftCount; i++) {
                const path = `/ingestion/${source.name}/provisions/${i}`;
                pathArray.push(path);
            }
        }

        console.log('WE ARE CREATING ZK PATHS');
        return async.waterfall([
            fin => async.each(pathArray, (path, next) =>
            this.zkClient.getData(path, err => {
                if (err) {
                    if (err.name !== 'NO_NODE') {
                        this.log.error('error getting zookeeper node', {
                            method: 'IngestionQueuePopulator.createZkPath',
                            error: err,
                        });
                        return next(err);
                    }
                }
                return this.zkClient.mkdirp(path, err => {
                    if (err) {
                        this.log.error('error creating zookeeper path', {
                            method: 'IngestionQueuePopulator.createZkPath',
                            zookeeperPath,
                            error: err,
                        });
                        return next(err);
                    }
                    // return next();
                });
            }), fin),
            fin => {
                console.log('INGESTION POPULATOR', IngestionPopulator);
                this.ingestionPopulator.populateIngestion({}, (err, res) => {
                    console.log('WE ARE POPULATING WITH INGESTION');
                    return fin(err, res);
                });
            },
        ], cb);
    }


    _setupZookeeper(done) {
        const populatorZkPath = this.qpConfig.zookeeperPath;
        const zookeeperUrl =
            `${this.zkConfig.connectionString}${populatorZkPath}`;
        this.log.info('opening zookeeper connection for persisting ' +
            'populator state', { zookeeperUrl });
        this.zkClient = zookeeper.createClient(zookeeperUrl, {
            autoCreateNamespace: this.zkConfig.autoCreateNamespace,
        });
        this.zkClient.connect();
        this.zkClient.once('error', done);
        this.zkClient.once('ready', () => {
            this.zkClient.removeAllListeners('error');
            done();
        });
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
        this.log.info('publishing entry',
                       { entryBucket: entry.bucket, entryKey: entry.key });
        this.publish(this.config.topic,
                     `${entry.bucket}/${entry.key}`,
                     JSON.stringify(entry));
    }
}

module.exports = IngestionQueuePopulator;
