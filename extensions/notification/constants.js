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
    supportedSaslProtocols: ['SASL_PLAINTEXT', 'SASL_SSL'],
    // SASL mechanism name kafka expects for kerberos, on both client stacks
    saslGssapiMechanism: 'GSSAPI',
    // where a kerberos destination's credential comes from: 'keytab' lets
    // MIT get the ticket from the keytab on demand, 'ccache' expects the
    // ticket to be in the credential cache collection already
    kerberosCredentialSources: ['keytab', 'ccache'],
    // producer stack serving kerberos destinations
    kerberosProducers: ['rdkafka', 'kafkajs'],
    // where the delivery workers read from: today's internal topic, matching
    // each event themselves, or the destination-keyed delivery topic
    deliveryPoolSources: ['internal', 'delivery'],
    supportedScramMechanisms: ['SHA-256', 'SHA-512']
};

module.exports = constants;
