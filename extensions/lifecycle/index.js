const LifecycleConfigValidator = require('./LifecycleConfigValidator');
const LifecycleOplogPopulatorUtils = require('./LifecycleOplogPopulatorUtils');

module.exports = {
    name: 'lifecycle',
    version: '1.0.0',
    configValidator: LifecycleConfigValidator,
    // we need a dynamic require to avoid a circular dependency
    queuePopulatorExtension: () => require('./LifecycleQueuePopulator'),
    oplogPopulatorUtils: LifecycleOplogPopulatorUtils,
};
