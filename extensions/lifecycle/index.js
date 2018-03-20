const LifecycleConfigValidator = require('./LifecycleConfigValidator');
const LifecycleQueuePopulator = require('./LifecycleQueuePopulator');

module.exports = {
    name: 'lifecycle',
    version: '1.0.0',
    configValidator: LifecycleConfigValidator,
    queuePopulatorExtension: LifecycleQueuePopulator,
};
