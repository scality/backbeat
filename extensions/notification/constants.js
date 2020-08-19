const constants = {
    extensionName: 'notification',
    nameFilter: {
        prefix: 'prefix',
        suffix: 'suffix',
    },
    zkBucketNotificationPath: '/bucket-notification',
    bucketNotifConfigPropName: 'notificationConfiguration',
    notificationEventPropName: 'originOp',
    eventTimePropName: 'last-modified',
    zkConfigParentNode: 'config',
};

module.exports = constants;
