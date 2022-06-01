const GarbageCollectorConfigValidator =
          require('./GarbageCollectorConfigValidator');
const GarbageCollectorQueuePopulator = require('./GarbageCollectorQueuePopulator');

module.exports = {
    name: 'gc',
    version: '1.0.0',
    configValidator: GarbageCollectorConfigValidator,
    queuePopulatorExtension: GarbageCollectorQueuePopulator,
};
