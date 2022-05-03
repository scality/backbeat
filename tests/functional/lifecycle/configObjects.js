const bucketTasksTopic = 'bucket-tasks';
const objectTasksTopic = 'object-tasks';

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
    auth: {
        type: 'assumeRole',
        roleName: 'role',
        sts: {},
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
    rules: {
        expiration: {
            enabled: true,
        },
    },
};

const repConfig = {
    destination: {
        bootstrapList: [],
    },
};

const s3Config = {};

const testTimeout = 30000;

module.exports = {
    bucketTasksTopic,
    objectTasksTopic,
    zkConfig,
    kafkaConfig,
    lcConfig,
    repConfig,
    s3Config,
    testTimeout,
};
