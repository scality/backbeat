const NotificationConfigValidator = require('./NotificationConfigValidator');
const NotificationQueuePopulator = require('./NotificationQueuePopulator');

module.exports = {
    name: 'notification',
    version: '1.0.0',
    configValidator: NotificationConfigValidator,
    queuePopulatorExtension: NotificationQueuePopulator,
};
