const assert = require('assert');
const fakeLogger = require('../../utils/fakeLogger');
const { splitter } = require('arsenal').constants;

const LifecycleConductor = require(
    '../../../extensions/lifecycle/conductor/LifecycleConductor');

const zkConfig = {
    zookeeper: {
        connectionString: '127.0.0.1:2181',
        autoCreateNamespace: true,
    },
};
const kafkaConfig = { hosts: '127.0.0.1:9092' };
const repConfig = {
    dataMoverTopic: 'backbeat-data-mover-spec',
};

const accountName1 = 'account1';
const account1 = 'ab288756448dc58f61482903131e7ae533553d20b52b0e2ef80235599a1b9143';
const account2 = 'cd288756448dc58f61482903131e7ae533553d20b52b0e2ef80235599a1b9144';
const bucket1 = 'bucket1';
const bucket2 = 'bucket2';

class Queue {
    constructor() {
        this.tasks = [];
    }

    push(task) {
        // task can be either an object or an array of object.
        if (Array.isArray(task)) {
            this.tasks.push(...task);
        } else {
            this.tasks.push(task);
        }
    }

    list() {
        return this.tasks;
    }

    length() {
        return this.tasks.length;
    }

    clean() {
        this.tasks = [];
    }
}

function makeLifecycleConductor(options, markers) {
    const { bucketsDenied, accountsDenied, bucketSource } = options;
    const lifecycleConfig = {
        zookeeperPath: '/test/lifecycle',
        conductor: {
            bucketSource,
            filter: {
                deny: {
                    buckets: bucketsDenied,
                    accounts: accountsDenied,
                },
            },
        },
    };

    const lcConductor = new LifecycleConductor(zkConfig.zookeeper,
        kafkaConfig, lifecycleConfig, repConfig);

    lcConductor._zkClient = {
        getChildren: (a, b, cb) => {
            cb(null, [
                `${account1}:bucketuid123:${bucket1}`,
                `${account2}:bucketuid456:${bucket2}`,
            ]);
        },
    };

    lcConductor._bucketClient = {
        listObject: (a, b, { marker }, cb) => {
            if (marker) {
                markers.push(marker);
                return cb(null, JSON.stringify({
                    Contents: [
                        {
                            key: `${account2}..|..${bucket2}`,
                        },
                    ],
                }));
            }
            return cb(null, JSON.stringify({
                Contents: [
                    {
                        key: `${account1}..|..${bucket1}`,
                    },
                ],
                IsTruncated: true,
            }));
        },
    };

    return lcConductor;
}

describe('LifecycleConductor: listBuckets', () => {
    const queue = new Queue();

    beforeEach(() => {
        queue.clean();
    });

    it('should list buckets from zookeeper', done => {
        const lcConductor = makeLifecycleConductor({
            bucketSource: 'zookeeper',
        });

        lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
            assert.strictEqual(length, 2);
            assert.strictEqual(queue.length(), 2);

            const expectedQueue = [
                {
                    canonicalId: account1,
                    bucketName: bucket1,
                },
                {
                    canonicalId: account2,
                    bucketName: bucket2,
                },
            ];
            assert.deepStrictEqual(queue.list(), expectedQueue);
            done();
        });
    });

    it('should list buckets from bucketd', done => {
        const markers = [];
        const lcConductor = makeLifecycleConductor({
            bucketSource: 'bucketd',
        }, markers);

        lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
            assert.strictEqual(length, 2);
            assert.strictEqual(queue.length(), 2);

            const expectedQueue = [
                {
                    canonicalId: account1,
                    bucketName: bucket1,
                },
                {
                    canonicalId: account2,
                    bucketName: bucket2,
                },
            ];
            assert.deepStrictEqual(queue.list(), expectedQueue);
            // test the listing optimization was not applied.
            assert.deepStrictEqual(markers, [`${account1}${splitter}${bucket1}`]);
            done();
        });
    });

    it('should filter by bucket when listing from zookeeper', done => {
        const lcConductor = makeLifecycleConductor({
            bucketsDenied: [bucket1],
            bucketSource: 'zookeeper',
        });

        lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
            assert.strictEqual(length, 1);
            assert.strictEqual(queue.length(), 1);

            const expectedQueue = [
                {
                    canonicalId: account2,
                    bucketName: bucket2,
                },
            ];
            assert.deepStrictEqual(queue.list(), expectedQueue);
            done();
        });
    });

    it('should filter by bucket when listing from bucketd', done => {
        const markers = [];
        const lcConductor = makeLifecycleConductor({
            bucketsDenied: [bucket1],
            bucketSource: 'bucketd',
        }, markers);

        lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
            assert.strictEqual(length, 1);
            assert.strictEqual(queue.length(), 1);

            const expectedQueue = [
                {
                    canonicalId: account2,
                    bucketName: bucket2,
                },
            ];
            assert.deepStrictEqual(queue.list(), expectedQueue);
            // test the listing optimization was not applied.
            assert.deepStrictEqual(markers, [`${account1}${splitter}${bucket1}`]);
            done();
        });
    });

    it('should filter by account when listing from zookeeper', done => {
        const lcConductor = makeLifecycleConductor({
            accountsDenied: [`${accountName1}:${account1}`],
            bucketSource: 'zookeeper',
        });

        lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
            assert.strictEqual(length, 1);
            assert.strictEqual(queue.length(), 1);

            const expectedQueue = [
                {
                    canonicalId: account2,
                    bucketName: bucket2,
                },
            ];
            assert.deepStrictEqual(queue.list(), expectedQueue);
            done();
        });
    });

    it('should filter by account when listing from bucketd', done => {
        const markers = [];
        const lcConductor = makeLifecycleConductor({
            accountsDenied: [`${accountName1}:${account1}`],
            bucketSource: 'bucketd',
        }, markers);

        lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
            assert.strictEqual(length, 1);
            assert.strictEqual(queue.length(), 1);

            const expectedQueue = [
                {
                    canonicalId: account2,
                    bucketName: bucket2,
                },
            ];
            assert.deepStrictEqual(queue.list(), expectedQueue);
            // test the listing optimization was applied
            assert.deepStrictEqual(markers, [`${account1};`]);
            done();
        });
    });

    it('should filter by account and bucket when listing from zookeeper', done => {
        const lcConductor = makeLifecycleConductor({
            accountsDenied: [`${accountName1}:${account1}`],
            bucketsDenied: [bucket2],
            bucketSource: 'zookeeper',
        });

        lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
            assert.strictEqual(length, 0);
            assert.strictEqual(queue.length(), 0);

            const expectedQueue = [];
            assert.deepStrictEqual(queue.list(), expectedQueue);
            done();
        });
    });

    it('should filter by account and bucket when listing from bucketd', done => {
        const markers = [];
        const lcConductor = makeLifecycleConductor({
            accountsDenied: [`${accountName1}:${account1}`],
            bucketsDenied: [bucket2],
            bucketSource: 'bucketd',
        }, markers);

        lcConductor.listBuckets(queue, fakeLogger, (err, length) => {
            assert.strictEqual(length, 0);
            assert.strictEqual(queue.length(), 0);

            const expectedQueue = [];
            assert.deepStrictEqual(queue.list(), expectedQueue);
            // test the listing optimization was not applied.
            assert.deepStrictEqual(markers, [`${account1}${splitter}${bucket1}`]);
            done();
        });
    });
});
