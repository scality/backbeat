const constants = {
    extensionName: 'notification',
    nameFilter: {
        prefix: 'Prefix',
        suffix: 'Suffix',
    },
    bucketNotifConfigPropName: 'notificationConfiguration',
    arn: {
        partition: 'scality',
        service: 'bucketnotif',
    },
    authFilesFolder: 'ssl',
    supportedAuthTypes: ['kerberos'],
    deleteEvent: 's3:ObjectRemoved:Delete',
    replicationFailedEvent: 's3:Replication:OperationFailedReplication',
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
    bucketMetastore: '__metastore',
};

module.exports = constants;
