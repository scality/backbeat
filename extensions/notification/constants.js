const constants = {
    extensionName: 'notification',
    nameFilter: {
        prefix: 'Prefix',
        suffix: 'Suffix',
    },
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
    eventVersion: '1.0',
    eventSource: 'scality:s3',
    eventS3SchemaVersion: '1.0',
    configManager: {
        concurrency: 50,
        maxListeners: 0,
    },
};

module.exports = constants;
