/* eslint-disable no-param-reassign */
const queuePopulatorUtils = {
    queueBatch: (queuePopulator, taskState, qpConfig, log) => {
        if (taskState.batchInProgress) {
            log.warn('skipping batch: previous one still in progress');
            return undefined;
        }
        log.debug('start queueing batch');
        taskState.batchInProgress = true;
        const maxRead = qpConfig.batchMaxRead;
        queuePopulator.processAllLogEntries({ maxRead }, (err, counters) => {
            taskState.batchInProgress = false;
            if (err) {
                log.error('an error occurred during populating', {
                    method: 'QueuePopulator::task.queueBatch',
                    error: err,
                });
                return undefined;
            }
            const logFunc = (counters.some(counter => counter.readRecords > 0) ?
                log.info : log.debug).bind(log);
            logFunc('population batch finished', { counters });
            return undefined;
        });
        return undefined;
    },
};
/* eslint-enable no-param-reassign */

module.exports = queuePopulatorUtils;
