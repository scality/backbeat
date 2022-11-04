const assert = require('assert');
const werelogs = require('werelogs');
const logger = new werelogs.Logger('ListRecordStream');
const ListRecordStream =
    require('../../../../../lib/queuePopulator/KafkaLogConsumer/ListRecordStream');

const changeStreamDocument = {
    ns: {
        db: 'metadata',
        coll: 'example-bucket',
    },
    documentKey: {
        _id: 'example-key',
    },
    operationType: 'insert',
    fullDocument: {
        value: {
            field: 'value'
        }
    }
};
const kafkaMessage = {
    value: Buffer.from(JSON.stringify(changeStreamDocument)),
    timestamp: Date.now(),
    size: 2,
    topic: 'oplog-topic',
    offset: 1337,
    partition: 0,
    key: Buffer.from('key'),
};

const InvalidKafkaMessage = {
    value: Buffer.from(''),
    timestamp: Date.now(),
    size: 2,
    topic: 'oplog-topic',
    offset: 1337,
    partition: 0,
    key: Buffer.from('key'),
};

describe('ListRecordStream', () => {
    let listRecordStream;
    beforeEach(() => {
        listRecordStream = new ListRecordStream(logger);
    });

    describe('_getType', () => {
        [
            {
                opType: 'insert',
                expected: 'put'
            },
            {
                opType: 'update',
                expected: 'put'
            },
            {
                opType: 'replace',
                expected: 'put'
            },
            {
                opType: 'delete',
                expected: 'delete'
            },
            {
                opType: 'unsupported',
                expected: undefined
            }].forEach(params => {
                const { opType, expected } = params;
                it(`Should return correct operation type (${opType})`, done => {
                    const type = listRecordStream._getType(opType);
                    assert.strictEqual(type, expected);
                    return done();
                });
            });
    });

    describe('_transform', () => {
        it('Should correct format entry', done => {
            listRecordStream.write(kafkaMessage);
            listRecordStream.once('data', data => {
                assert.deepEqual(data, {
                    timestamp: new Date(kafkaMessage.timestamp),
                    db: 'example-bucket',
                    entries: [{
                        key: 'example-key',
                        type: 'put',
                        value: JSON.stringify({
                            field: 'value'
                        }),
                    }],
                });
                return done();
            });
        });
        it('Should skip record if format is invalid', done => {
            listRecordStream.write(InvalidKafkaMessage);
            listRecordStream.write(kafkaMessage);
            listRecordStream.once('data', data => {
                // Streams guarantee that data is kept in the
                // same order when writing and reading it.
                // This means that if the function doesn't work
                // as intended and processed the invalid
                // event it should be read in first by this event
                // handler which'll fail the test
                assert.deepEqual(data, {
                    timestamp: new Date(kafkaMessage.timestamp),
                    db: 'example-bucket',
                    entries: [{
                        key: 'example-key',
                        type: 'put',
                        value: JSON.stringify({
                            field: 'value'
                        }),
                    }],
                });
                return done();
            });
        });
    });
});
