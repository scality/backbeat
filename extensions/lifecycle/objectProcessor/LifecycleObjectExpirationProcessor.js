'use strict'; // eslint-disable-line

const Logger = require('werelogs').Logger;

const LifecycleObjectProcessor = require('./LifecycleObjectProcessor');
const LifecycleDeleteObjectTask =
      require('../tasks/LifecycleDeleteObjectTask');
const LifecycleUpdateExpirationTask = require('../tasks/LifecycleUpdateExpirationTask');

class LifecycleObjectExpirationProcessor extends LifecycleObjectProcessor {

    /**
     * Constructor of LifecycleObjectProcessor
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {String} zkConfig.connectionString - zookeeper connection string
     *  as "host:port[/chroot]"
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} [kafkaConfig.backlogMetrics] - param object to
     * publish kafka topic metrics to zookeeper (see {@link
     * BackbeatConsumer} constructor)
     * @param {Object} lcConfig - lifecycle configuration object
     * @param {String} lcConfig.auth - authentication info
     * @param {String} lcConfig.objectTasksTopic - lifecycle object topic name
     * @param {Object} lcConfig.objectProcessor - kafka consumer object
     * @param {String} lcConfig.objectProcessor.groupId - kafka
     * consumer group id
     * @param {Number} [lcConfig.objectProcessor.concurrency] - number
     *  of max allowed concurrent operations
     * @param {Object} s3Config - S3 configuration
     * @param {Object} s3Config.host - s3 endpoint host
     * @param {Number} s3Config.port - s3 endpoint port
     * @param {String} [transport="http"] - transport method ("http"
     *  or "https")
     */
    constructor(zkConfig, kafkaConfig, lcConfig, s3Config, transport = 'http') {
        super(zkConfig, kafkaConfig, lcConfig, s3Config, transport);
        this._log = new Logger(this.getId());
    }

    getId() {
        return 'Backbeat:Lifecycle:ObjectExpirationProcessor';
    }

    getProcessConfig(lcConfig) {
        return lcConfig.objectProcessor;
    }

    getAuthConfig(lcConfig) {
        if (lcConfig.objectProcessor.auth) {
            return lcConfig.objectProcessor.auth;
        }

        return lcConfig.auth;
    }

    getTask(actionEntry) {
        const actionType = actionEntry.getActionType();

        switch (actionType) {
            case 'deleteObject':
            case 'deleteMPU':
                return new LifecycleDeleteObjectTask(this);
            case 'gc':
                return new LifecycleUpdateExpirationTask(this);
            default:
                this._log.warn(
                    `skipped unsupported action ${actionType}`,
                    actionEntry.getLogInfo(),
                );
                return null;
        }
    }
}

module.exports = LifecycleObjectExpirationProcessor;
