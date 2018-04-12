const GenericConfigValidator = require('./GenericConfigValidator');
const GenericQueuePopulator = require('./GenericQueuePopulator');

module.exports = {
    name: 'generic',
    version: '1.0.0',
    configValidator: GenericConfigValidator,
    queuePopulatorExtension: GenericQueuePopulator,
};
