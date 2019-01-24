const werelogs = require('werelogs');
const { HealthProbeServer } = require('arsenal').network.probe;

const HelloWorldProcessor = require('./HelloWorldProcessor');
const config = require('../../../conf/Config');
const { initManagement } = require('../../../lib/management/index');

const kafkaConfig = config.kafka;
const hwConfig = config.extensions.helloWorld;
const sourceReplicationConfig = config.extensions.replication.source;

const log = new werelogs.Logger('HelloWorldProcessor:task');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

let helloworldProcessor;

// healthcheck used in Zenko kubernetes deployment
function loadHealthcheck() {
    healthServer.onReadyCheck(() => {
        if (!helloworldProcessor.isReady()) {
            log.error('HWQueueProcessor is not ready.');
            return false;
        }
        return true;
    });
    log.info('starting health probe server');
    healthServer.start();
}

// the internal management layer that retrieves service accounts, updates on
// config changes from Zenko Orbit UI, etc
function loadManagementDatabase() {
    initManagement({
        // NOTE: borrowing existing service account for demo purposes only
        serviceName: 'replication',
        serviceAccount: sourceReplicationConfig.auth.account,
    }, error => {
        if (error) {
            log.error('could not load management db', { error });
            setTimeout(loadManagementDatabase, 5000);
            return;
        }
        log.info('management init done');

        helloworldProcessor = new HelloWorldProcessor(
            kafkaConfig, hwConfig, sourceReplicationConfig.auth);
        helloworldProcessor.start();
        loadHealthcheck();
    });
}

loadManagementDatabase();
