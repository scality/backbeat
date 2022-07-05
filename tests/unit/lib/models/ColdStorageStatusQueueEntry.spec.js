const assert = require('assert');

const ColdStorageStatusQueueEntry = require('../../../../lib/models/ColdStorageStatusQueueEntry');

const nonVersioned = Buffer.from(`{
"op":"archive",
"bucketName":"testBucket",
"objectKey":"testObj",
"accountId":"834789881858",
"archiveInfo":{"archiveId":"da80b6dc-280d-4dce-83b5-d5b40276e321","archiveVersion":5166759712787974},
"requestId":"060733275a408411c862"
}`);

const versioned = Buffer.from(`{
"op":"archive",
"bucketName":"testBucket",
"objectKey":"testObj",
"objectVersion":"testversionkey",
"accountId":"834789881858",
"archiveInfo":{"archiveId":"da80b6dc-280d-4dce-83b5-d5b40276e321","archiveVersion":5166759712787974},
"requestId":"060733275a408411c862"
}`);

const invalidObj = Buffer.from(`{
"bucketName":"testBucket",
"objectKey":"testObj",
"accountId":"834789881858",
"archiveInfo":{"archiveId":"da80b6dc-280d-4dce-83b5-d5b40276e321","archiveVersion":5166759712787974},
"requestId":"060733275a408411c862"
}`);

const malformed = Buffer.from(`
"bucketName":"testBucket",
"objectKey":"testObj",
"accountId":"834789881858",
"archiveInfo":{"archiveId":"da80b6dc-280d-4dce-83b5-d5b40276e321","archiveVersion":5166759712787974},
"requestId":"060733275a408411c862"
}`);

describe('ColdStorageStatusQueueEntry', () => {
    [
        {
            msg: 'should parse non-versioned object status entry',
            input: nonVersioned,
            error: null,
        },
        {
            msg: 'should parse versioned object status entry',
            input: versioned,
            error: null,
        },
        {
            msg: 'should return error for invalid messages',
            input: invalidObj,
            error: { message: 'invalid status kafka entry' },
        },
        {
            msg: 'should return error for malformed messages',
            input: malformed,
            error: { message: 'malformed JSON in kafka entry' },
        },
    ].forEach(({ msg, input, error }) => it(msg, () => {
        const res = ColdStorageStatusQueueEntry.createFromKafkaEntry({ value: input });

        if (error) {
            assert.strictEqual(res.error.message, error.message);
        } else {
            assert.ifError(res.error);
        }
    }));
});
