const ReplicationAPI =
      require('../../../extensions/replication/ReplicationAPI');

const assert = require('assert');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');

const fakeLogger = require('../../utils/fakeLogger');
const bucketName = 'transition-to-dmf';
const owner = '4f5a1a4bd769fd6e4ebca87b96c86a621ebb9c8be0c012f291757410c55a36f7';
const objectKey = 'n2bv20';
const versionId = '39383334313437353030313233353939393939395247303031202038393336382e38';
const eTag = '"f611f77f57aa931a6475ae3fbb8e6d33"';
const lastModified = '2022-07-22T21:23:18.761Z';
const toLocation = 'location-dmf-v1';
const originLabel = 'lifecycle';
const fromLocation = 'aws-location';
const contentLength = 315;
const resultsTopic = 'backbeat-lifecycle-object-tasks';
const accountId = '507132247041';

describe('ReplicationAPI', () => {
    let messages;
    const mockProducer = {
        sendToTopic: (topic, [{ message }], cb) => {
            const entry = JSON.parse(message);
            messages.push({ topic, entry });
            process.nextTick(() => cb(null, [{}]));
            return;
        },
    };

    beforeEach(() => {
        messages = [];
    });

    describe('::sendDataMoverAction ', () => {
        it('should publish to archive topic', done => {
            const action = ActionQueueEntry.create('copyLocation');
            action
                .setAttribute('target', {
                    accountId,
                    owner,
                    bucket: bucketName,
                    key: objectKey,
                    version: versionId,
                    eTag,
                    lastModified,
                })
                .setAttribute('toLocation', toLocation)
                .setAttribute('metrics', {
                    origin: originLabel,
                    fromLocation,
                    contentLength,
                })
                .setResultsTopic(resultsTopic);
            ReplicationAPI.sendDataMoverAction(mockProducer, action, fakeLogger, err => {
                assert.ifError(err);
                const expectedMessage = [
                    {
                        topic: 'cold-archive-req-location-dmf-v1',
                        entry: {
                            accountId,
                            bucketName,
                            objectKey,
                            objectVersion: versionId,
                            size: contentLength,
                            eTag,
                        },
                    },
                ];
                assert.deepStrictEqual(messages, expectedMessage);
                done();
                return;
            });
        });
    });
});
