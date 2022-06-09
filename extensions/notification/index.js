const NotificationConfigValidator = require('./NotificationConfigValidator');
const NotificationQueuePopulator = require('./NotificationQueuePopulator');
const NotificationOplogPopulatorUtils = require('./NotificationOplogPopulatorUtils');

module.exports = {
    name: 'notification',
    version: '1.0.0',
    configValidator: NotificationConfigValidator,
    queuePopulatorExtension: NotificationQueuePopulator,
    oplogPopulatorUtils: NotificationOplogPopulatorUtils,
};
