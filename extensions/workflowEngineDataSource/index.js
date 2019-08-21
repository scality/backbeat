const WorkflowEngineDataSourceConfigValidator =
      require('./WorkflowEngineDataSourceConfigValidator');
const WorkflowEngineDataSourceQueuePopulator =
      require('./WorkflowEngineDataSourceQueuePopulator');

module.exports = {
    name: 'workflow-engine-data-source',
    version: '1.0.0',
    configValidator: WorkflowEngineDataSourceConfigValidator,
    queuePopulatorExtension: WorkflowEngineDataSourceQueuePopulator,
};
