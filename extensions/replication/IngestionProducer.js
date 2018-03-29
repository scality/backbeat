// MAKE CLASS AN EXTENSION OF QUEUEPOPULATOREXTENSION

const async = require('async');

const Logger = require('werelogs').Logger;

const QueuePopulatorExtension  = require('../../lib/queuePopulator/QueuePopulatorExtension');
const zookeeper = require('../../lib/clients/zookeeper');
const ProvisionDispatcher = require('../../lib/provisioning/ProvisionDispatcher');
const RaftLogReader = require('../../lib/queuePopulator/RaftLogReader');
const BucketFileLogReader = require('../../lib/queuePopulator/BucketFileLogReader');
const constants = require('../../constants');

class IngestionProducer extends QueuePopulatorExtension{
    createZkPath(node, cb) {
        console.log('WE CREATING A PATH');
        const { zookeeperPath } = this.extConfig;
        const path = `/ingestion-producer/raft-id-dispatcher/provisions/${node}`;
        return this.zkClient.getData(path, err => {
            if (err) {
                if (err.name !== 'NO_NODE') {
                    this.log.error('could not get zookeeper node path', {
                        method: 'IngestionProducer.createZkPath',
                        error: err,
                    });
                    return cb(err);
                }
                return this.zkClient.mkdirp(path, err => {
                    if (err) {
                        this.log.error('could not create path in zookeeper', {
                            method: 'IngestionProducer.createZkPath',
                            zookeeperPath,
                            error: err,
                        });
                        return cb(err);
                    }
                });
            }
        });
    }

    _setupZookeeper(done) {
        const populatorZkPath = this.qpConfig.zookeeperPath;
        const zookeeperUrl =
            `${this.zkConfig.connectionString}${populatorZkPath}`;
        this.log.info('opening zookeeper connection for persisting ' +
                      'populator state',
                      { zookeeperUrl });
        this.zkClient = zookeeper.createClient(zookeeperUrl, {
            autoCreateNamespace: this.zkConfig.autoCreateNamespace,
        });
        this.zkClient.connect();
        this.zkClient.once('error', done);
        this.zkClient.once('ready', () => {
            // just in case there would be more 'error' events emitted
            this.zkClient.removeAllListeners('error');
            done();
        });
    }
    //
    // filter(entry, bucket, key) {
    //     console.log('filtering entry!', bucket, key);
    //     console.log(entry[0]);
    //     this.publish(this.extConfig.topic, `${bucket}/${key}`,
    //         JSON.stringify(entry[0]));
    // }
}


module.exports = IngestionProducer;
