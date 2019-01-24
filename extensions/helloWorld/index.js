const HelloWorldConfigValidator = require('./HelloWorldConfigValidator');
const HelloWorldQueuePopulator = require('./HelloWorldQueuePopulator');

module.exports = {
    name: 'hello-world',
    version: '1.0.0',
    configValidator: HelloWorldConfigValidator,
    queuePopulatorExtension: HelloWorldQueuePopulator,
};
