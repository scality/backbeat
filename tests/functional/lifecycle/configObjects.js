const bucketTasksTopic = 'bucket-tasks';
const objectTasksTopic = 'object-tasks';
const transitionTasksTopic = 'transition-tasks';

const zkConfig = {
    connectionString: 'localhost:2181',
};

const kafkaConfig = {
    hosts: 'localhost:9092',
    backlogMetrics: {
        zkPath: '/backlog',
    },
};

const lcConfig = {
    forceLegacyListing: true,
    auth: {
        type: 'assumeRole',
        roleName: 'role',
        sts: {},
    },
    conductor: {
        cronRule: '*/5 * * * * *',
        concurrentIndexesBuildLimit: 2,
        bucketSource: 'mongodb',
    },
    bucketProcessor: {
        groupId: `bucket-processor-test-${Math.random()}`,
    },
    objectProcessor: {
        groupId: `object-processor-test-${Math.random()}`,
    },
    transitionProcessor: {
        groupId: `transition-processor-test-${Math.random()}`,
    },
    bucketTasksTopic,
    objectTasksTopic,
    transitionTasksTopic,
    rules: {
        expiration: {
            enabled: true,
        },
    },
    supportedLifecycleRules: [
        'expiration',
        'noncurrentVersionExpiration',
        'abortIncompleteMultipartUpload',
        'transitions',
        'noncurrentVersionTransition'
    ]
};

const repConfig = {
    destination: {
        bootstrapList: [],
    },
};

const s3Config = {};

const mongoConfig = {
    replicaSetHosts: 'localhost:27017,localhost:27018,localhost:27019',
    writeConcern: 'majority',
    replicaSet: 'rs0',
    readPreference: 'primary',
    database: 'metadata'
};

const testTimeout = 30000;

const timeOptions = {
    expireOneDayEarlier: false,
    transitionOneDayEarlier: false,
    timeProgressionFactor: 1,
};

module.exports = {
    bucketTasksTopic,
    objectTasksTopic,
    transitionTasksTopic,
    zkConfig,
    kafkaConfig,
    lcConfig,
    repConfig,
    s3Config,
    testTimeout,
    mongoConfig,
    timeOptions,
};
