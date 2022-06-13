const LifecycleConfigValidator = require('./LifecycleConfigValidator');

module.exports = {
    name: 'lifecycle',
    version: '1.0.0',
    configValidator: LifecycleConfigValidator,
    // we need a dynamic require to avoid a circular dependency
    queuePopulatorExtension: () => require('./LifecycleQueuePopulator'),
};
