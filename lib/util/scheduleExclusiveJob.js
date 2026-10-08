const schedule = require('node-schedule');

/**
 * Wraps an async task so that a call made while a previous one is still
 * running is skipped, and so that a failure is logged instead of escaping
 * as an unhandled rejection.
 * @param {Function} task async task
 * @param {Logger} logger logger
 * @param {string} method name of the task, for logs
 * @returns {Function} async function running the task exclusively
 */
function makeExclusive(task, logger, method) {
    let inProgress = false;
    return async () => {
        if (inProgress) {
            return;
        }
        inProgress = true;
        try {
            await task();
        } catch (err) {
            logger.error('scheduled task failed', { method, error: err.description || err.message });
        }
        inProgress = false;
    };
}

/**
 * Schedules an async task on a cron rule, never running two instances of it
 * concurrently.
 * @param {string} cronRule cron rule
 * @param {Function} task async task
 * @param {Logger} logger logger
 * @param {string} method name of the task, for logs
 * @returns {schedule.Job} scheduled job
 */
function scheduleExclusiveJob(cronRule, task, logger, method) {
    return schedule.scheduleJob(cronRule, makeExclusive(task, logger, method));
}

module.exports = {
    makeExclusive,
    scheduleExclusiveJob,
};
