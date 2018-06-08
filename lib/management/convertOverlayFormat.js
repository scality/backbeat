/**
 * convert legacy configuration format into new format (for legacy
 * replication config)
 *
 * @param {object} conf - overlay config object
 * @return {object} - converted overlay config object
 *
 * @note this function should be removed once the new format is
 * adopted for replication
 */
function convertOverlayFormat(conf) {
    if (!conf.replicationStreams) {
        return conf;
    }
    const convConf = Object.assign({}, conf);
    const replicationWorkflows = {};
    conf.replicationStreams.forEach(stream => {
        const workflow = Object.assign({}, stream);
        workflow.workflowId = stream.streamId;
        delete workflow.streamId;
        if (!replicationWorkflows[stream.source.bucketName]) {
            replicationWorkflows[stream.source.bucketName] = [];
        }
        replicationWorkflows[stream.source.bucketName].push(workflow);
    });
    if (!convConf.workflows) {
        convConf.workflows = {};
    }
    convConf.workflows.replication = replicationWorkflows;
    delete convConf.replicationStreams;
    return convConf;
}

module.exports = convertOverlayFormat;
