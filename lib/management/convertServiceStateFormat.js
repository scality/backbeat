/**
 * convert legacy service state format into new format (for legacy
 * replication state)
 *
 * @param {object} state - service state object
 * @return {object} - converted service state object
 *
 * @note this function should be removed once the new format is
 * adopted for replication
 */
function convertServiceStateFormat(state) {
    if (!state.streams) {
        return state;
    }
    const convState = Object.assign({}, state);
    const workflows = {};
    Object.keys(state.streams).forEach(streamId => {
        const stream = Object.assign({}, state.streams[streamId]);
        stream.workflowId = stream.streamId;
        delete stream.streamId;
        const bucketName = stream.source.bucketName;
        if (!workflows[bucketName]) {
            workflows[bucketName] = {};
        }
        workflows[bucketName][streamId] = stream;
    });
    convState.workflows = workflows;
    delete convState.streams;
    return convState;
}

module.exports = convertServiceStateFormat;
