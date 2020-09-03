const constants = {
    extensionName: 'notification',
    nameFilter: {
        prefix: 'prefix',
        suffix: 'suffix',
    },
    zkBucketNotificationPath: 'bucket-notification',
    bucketNotifConfigPropName: 'notificationConfiguration',
    zkConfigParentNode: 'config',
    arn: {
        partition: 'scality',
        service: 'bucketnotif',
    },
    authFilesFolder: 'ssl',
    supportedAuthTypes: ['kerberos'],
    deleteEvent: 's3:ObjectRemoved:Delete',
    eventMessageProperty: {
        dateTime: 'last-modified',
        eventType: 'originOp',
        region: 'dataStoreName',
        schemaVersion: 'md-model-version',
        size: 'content-length',
        versionId: 'versionId',
    },
    eventVersion: '0.1',
    eventSource: 'scality:s3',
};

module.exports = constants;
