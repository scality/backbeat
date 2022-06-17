const WorkflowEngineDataSourceConfigValidator =
      require('./WorkflowEngineDataSourceConfigValidator');

module.exports = {
    name: 'workflow-engine-data-source',
    version: '1.0.0',
    configValidator: WorkflowEngineDataSourceConfigValidator,
    queuePopulatorExtension: () => require('./WorkflowEngineDataSourceQueuePopulator'),
};
