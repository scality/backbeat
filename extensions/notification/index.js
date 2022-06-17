const NotificationConfigValidator = require('./NotificationConfigValidator');
const NotificationOplogPopulatorUtils = require('./NotificationOplogPopulatorUtils');

module.exports = {
    name: 'notification',
    version: '1.0.0',
    configValidator: NotificationConfigValidator,
    queuePopulatorExtension: () => require('./NotificationQueuePopulator'),
    oplogPopulatorUtils: NotificationOplogPopulatorUtils,
};
