const errors = require('arsenal').errors;

const { attachReqUids } = require('../../../lib/clients/utils');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');

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
    }

    _executeDeleteData(entry, log, done) {
        log.debug('action execution starts', entry.getLogInfo());
        const {
            locations,
            owner: canonicalId,
            accountId
        } = entry.getAttribute('target');
        const backbeatClient = this.getBackbeatClient(canonicalId, accountId);

        if (!backbeatClient) {
            log.error('failed to get backbeat client', {
                canonicalId,
                accountId,
            });
            return done(errors.InternalError
                .customizeDescription('Unable to obtain client'));
        }

        const req = backbeatClient.batchDelete({
            Locations: locations.map(location => ({
                key: location.key,
                dataStoreName: location.dataStoreName,
                size: location.size,
                dataStoreVersionId: location.dataStoreVersionId,
            })),
            IfUnmodifiedSince: entry.getAttribute('source.lastModified'),
            Bucket: entry.getAttribute('source.bucket'),
            Key: entry.getAttribute('source.objectKey'),
            StorageClass: entry.getAttribute('source.storageClass'),
            Tags: JSON.stringify({
                'scal-delete-marker': 'true',
                'scal-delete-service': entry.getAttribute('serviceName'),
            }),
        });
        attachReqUids(req, log);
        return req.send(err => {
            entry.setEnd(err);
            log.info('action execution ended', entry.getLogInfo());
            if (err && err.statusCode === 412) {
                log.info('precondition for garbage collection was not met',
                    Object.assign({
                        method: 'LifecycleObjectTask._executeDeleteData',
                        lastModified: entry.getAttribute('source.lastModified'),
                    }, entry.getLogInfo()));
                return done();
            }
            if (err) {
                log.error('an error occurred on deleteData method to ' +
                          'backbeat route',
                          Object.assign({
                              method: 'LifecycleObjectTask._executeDeleteData',
                              error: err.message,
                              httpStatus: err.statusCode,
                          }, entry.getLogInfo()));
                return done(err);
            }
            return done();
        });
    }

    /**
     * Execute the action specified in kafka queue entry
     *
     * @param {ActionQueueEntry} entry - kafka queue entry object
     * @param {String} entry.action - entry action name (e.g. 'deleteData')
     * @param {Object} entry.target - entry action target object
     * @param {Function} done - callback funtion
     * @return {undefined}
     */

    processActionEntry(entry, done) {
        const log = this.logger.newRequestLogger();

        if (entry.getActionType() === 'deleteData') {
            return this._executeDeleteData(entry, log, done);
        }
        log.warn('skipped unsupported action', entry.getLogInfo());
        return process.nextTick(done);
    }
}

module.exports = GarbageCollectorTask;
