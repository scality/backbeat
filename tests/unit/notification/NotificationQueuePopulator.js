const assert = require('assert');
const werelogs = require('werelogs');

const NotificationQueuePopulator = require('../../../extensions/notification/NotificationQueuePopulator');

const log = new werelogs.Logger('NotificationQueuePopulator:test');
werelogs.configure({
    level: 'warn',
    dump: 'error',
});

const bucket = 'bucket1';
const objectKey = 'object1';
const value = {
    bucket,
};
const fallbackCommitTimestamp = '2022-10-12T00:01:02.003Z';
const mdTimestamp = '2023-10-12T00:01:02.003Z';
const fallbackVersionId = 'vid001';
const configTopic = 'topic1';
const bnConfigManager = {
    getConfig: () => ({
        bucket,
        notificationConfiguration: {
            queueConfig: [
                {
                    events: ['*'],
                },
            ],
        },
    }),
};

describe('NotificationQueuePopulator', () => {
    describe('_processObjectEntry', () => {
        it('any op: should use the event timestamp from MD if available', () => {
            let published = false;

            const qp = new NotificationQueuePopulator({
                logger: log,
                config: {
                    topic: configTopic,
                },
                bnConfigManager,
            });

            qp.publish = (topic, key, data) => {
                const parsed = JSON.parse(data);

                assert.deepStrictEqual(topic, configTopic);
                assert.deepStrictEqual(key, `${bucket}/${objectKey}`);
                assert.deepStrictEqual(parsed.dateTime, mdTimestamp);

                published = true;
            };

            const valueWithMD = {
                'originOp': 's3:ObjectCreated:Put',
                'last-modified': mdTimestamp,
                ...value,
            };

            const overheadFields = {
                commitTimestamp: fallbackCommitTimestamp,
            };
            qp._processObjectEntry(bucket, objectKey, valueWithMD, null, overheadFields);

            assert(published);
        });

        it('any op: should accept null version with non-empty version ids with isNull md', () => {
            let published = false;

            const qp = new NotificationQueuePopulator({
                logger: log,
                config: {
                    topic: configTopic,
                },
                bnConfigManager,
            });

            qp.publish = () => {
                published = true;
            };

            const valueWithMD = {
                originOp: 's3:ObjectCreated:Put',
                versionId: fallbackVersionId,
                isNull: true,
                ...value,
            };

            qp._processObjectEntry(bucket, `${objectKey}\0`, valueWithMD);

            assert(published);
        });

        it('delete: should use the fallback event timestamp', () => {
            const type = 'del';
            let published = false;

            const qp = new NotificationQueuePopulator({
                logger: log,
                config: {
                    topic: configTopic,
                },
                bnConfigManager,
            });

            qp.publish = (topic, key, data) => {
                const parsed = JSON.parse(data);

                assert.deepStrictEqual(topic, configTopic);
                assert.deepStrictEqual(key, `${bucket}/${objectKey}`);
                assert.deepStrictEqual(parsed.dateTime, fallbackCommitTimestamp);

                published = true;
            };

            const overheadFields = {
                commitTimestamp: fallbackCommitTimestamp,
            };
            qp._processObjectEntry(bucket, objectKey, value, type, overheadFields);

            assert(published);
        });

        it('delete: should use version id from overhead fields', () => {
            const type = 'del';
            let published = false;

            const qp = new NotificationQueuePopulator({
                logger: log,
                config: {
                    topic: configTopic,
                },
                bnConfigManager,
            });

            qp.publish = (topic, key, data) => {
                const parsed = JSON.parse(data);

                assert.deepStrictEqual(topic, configTopic);
                assert.deepStrictEqual(key, `${bucket}/${objectKey}`);
                assert.deepStrictEqual(parsed.versionId, fallbackVersionId);

                published = true;
            };

            const overheadFields = {
                commitTimestamp: fallbackCommitTimestamp,
                versionId: fallbackVersionId,
            };
            qp._processObjectEntry(bucket, objectKey, value, type, overheadFields);

            assert(published);
        });

        it('delete: should handle missing overhead fields', () => {
            const type = 'del';
            let published = false;

            const qp = new NotificationQueuePopulator({
                logger: log,
                config: {
                    topic: configTopic,
                },
                bnConfigManager,
            });

            qp.publish = (topic, key, data) => {
                const parsed = JSON.parse(data);

                assert.deepStrictEqual(topic, configTopic);
                assert.deepStrictEqual(key, `${bucket}/${objectKey}`);
                assert.deepStrictEqual(parsed.versionId, null);
                assert.deepStrictEqual(parsed.dateTime, null);

                published = true;
            };

            qp._processObjectEntry(bucket, objectKey, value, type);

            assert(published);
        });
    });
});
