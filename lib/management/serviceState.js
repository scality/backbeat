const async = require('async');
const errors = require('arsenal').errors;
const werelogs = require('werelogs');

const applyWorkflowUpdates = require('./applyWorkflowUpdates');
const convertOverlayFormat = require('./convertOverlayFormat');
const getWorkflowUpdates = require('./getWorkflowUpdates');

const logger = new werelogs.Logger('mdManagementServiceState');

const notImplementedErr = errors.NotImplemented.customizeDescription(
    `not implemented for management backend '${process.env.MANAGEMENT_BACKEND}'`);

function getServiceWorkflows(serviceName) {
    const workflows = [serviceName];
    // The lifecycle service manager handles both the expiration workflow and
    // the transition workflow.
    if (serviceName === 'lifecycle') {
        workflows.push('transition');
    }
    return workflows;
}

class BaseServiceState {
    /**
     *
     * @param {string} serviceName - name of the service to persist state for
     */
    constructor(serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * Converges actual service state with expected service state
     * @param {Object} conf - expected configuration
     * @param {Object} params - service state options
     * @param {string} params.serviceName - name of the service
     * @param {string} params.serviceAccount - service account to use when applying service state
     * @param {function} params.applyBucketWorkflows - function that effectively applies service state
     * @param {function} cb - function called back with error and result
     * @return {undefined}
     */
    apply(conf, params, cb) {
        if (!params.applyBucketWorkflows) {
            return process.nextTick(cb);
        }
        const convertedConf = convertOverlayFormat(conf);
        return async.waterfall([
            cb => this.load(cb),
            (currentState, cb) =>
                this._applyServiceState(convertedConf, params, currentState, cb),
        ], cb);
    }

    _applyServiceState(conf, params, currentState, cb) {
        const workflows = getServiceWorkflows(params.serviceName);
        async.each(workflows, (workflow, next) => {
            const configuredWorkflows = conf.workflows && conf.workflows[workflow];
            const currentWorkflows = currentState && currentState.workflows;
            const workflowUpdates =
                getWorkflowUpdates(configuredWorkflows, currentWorkflows);
            logger.info('applying workflow updates', { workflowUpdates });
            applyWorkflowUpdates(params, conf, currentState, workflowUpdates,
                logger, (err, newState) => {
                    if (err) {
                        return next(err);
                    }
                    if (newState) {
                        return this.save(newState, next);
                    }
                    logger.debug('workflows did not change, skip saving ' +
                        `${workflow} state`, { newState });
                    return next();
                });
        }, cb);
    }

    /**
     * Load, deserialize and return the state
     * @param {function} cb - function called back with error (if any) and loaded state
     * @return {undefined}
     */
    load(cb) {
        process.nextTick(cb, notImplementedErr);
    }

    /**
     * Save the state object
     * @param {Object} newState - state object to persist
     * @param {function} cb - function called back with error (if any)
     * @return {undefined}
     */
    save(newState, cb) {
        process.nextTick(cb, notImplementedErr);
    }

    /**
     * Get the initial empty state
     * @return {Object} the initial state
     */
    getInitialState() {
        return {
            workflows: {},
            overlayVersion: 0,
        };
    }
}

module.exports = {
    BaseServiceState,
};
