const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const { attachReqUids } = require('../../../lib/clients/utils');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { getAccountCredentials } =
          require('../../../lib/credentials/AccountCredentials');

class GarbageCollectorTask extends BackbeatTask {
    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {GarbageCollector} gc - garbage collector instance
     */
    constructor(gc) {
        super();
        const gcState = gc.getStateVars();
        Object.assign(this, gcState);

        this._setup();
    }

    _setup() {
        const accountCreds = getAccountCredentials(
            this.gcConfig.auth, this.logger);
        const s3 = this.s3Config;
        const transport = this.transport;
        this.logger.debug('creating backbeat client', { transport, s3 });
        this._backbeatClient = new BackbeatClient({
            endpoint: `${transport}://${s3.host}:${s3.port}`,
            credentials: accountCreds,
            sslEnabled: transport === 'https',
            httpOptions: { agent: this.httpAgent, timeout: 0 },
            maxRetries: 0,
        });
    }

    _executeDeleteData(entry, log, done) {
        const { locations } = entry.target;
        const req = this._backbeatClient.batchDelete({
            Locations: locations,
        });
        attachReqUids(req, log);
        return req.send(err => {
            if (err) {
                log.error('an error occurred on deleteData method to ' +
                          'backbeat route',
                          { method: 'LifecycleObjectTask._executeDeleteData',
                            error: err.message,
                            httpStatus: err.statusCode });
                return done(err);
            }
            return done();
        });
    }

    /**
     * Execute the action specified in kafka queue entry
     *
     * @param {Object} entry - kafka queue entry object
     * @param {String} entry.action - entry action name (e.g. 'deleteData')
     * @param {Object} entry.target - entry action target object
     * @param {Function} done - callback funtion
     * @return {undefined}
     */

    processQueueEntry(entry, done) {
        const log = this.logger.newRequestLogger();

        const { action, target } = entry;
        log.debug('processing garbage collector entry', { action, target });
        if (!target) {
            log.error('missing "target" in object queue entry', { entry });
            return process.nextTick(done);
        }
        if (action === 'deleteData') {
            return this._executeDeleteData(entry, log, done);
        }
        log.info('skipped unsupported action', { action, target });
        return process.nextTick(done);
    }
}

module.exports = GarbageCollectorTask;
