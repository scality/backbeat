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
const configTopic = 'topic1';

describe('NotificationQueuePopulator', () => {
    describe('_processObjectEntry', () => {
        it('should use the fallback event timestamp for deletes', () => {
            const type = 'del';
            let published = false;

            const qp = new NotificationQueuePopulator({
                logger: log,
                config: {
                    topic: configTopic,
                },
                bnConfigManager: {
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
                },
            });

            qp.publish = (topic, key, data) => {
                const parsed = JSON.parse(data);

                assert.deepStrictEqual(topic, configTopic);
                assert.deepStrictEqual(key, `${bucket}/${objectKey}`);
                assert.deepStrictEqual(parsed.dateTime, fallbackCommitTimestamp);

                published = true;
            };

            qp._processObjectEntry(bucket, objectKey, value, type, fallbackCommitTimestamp);

            assert(published);
        });

        it('should use the event timestamp from MD if available', () => {
            let published = false;

            const qp = new NotificationQueuePopulator({
                logger: log,
                config: {
                    topic: configTopic,
                },
                bnConfigManager: {
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
                },
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

            qp._processObjectEntry(bucket, objectKey, valueWithMD, null, fallbackCommitTimestamp);

            assert(published);
        });
    });
});
