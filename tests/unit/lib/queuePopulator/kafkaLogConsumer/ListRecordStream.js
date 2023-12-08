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
    clusterTime: {
        $timestamp: {
            t: 1701270357,
            i: 1,
        },
    },
    fullDocument: {
        value: {
            field: 'value'
        }
    }
};
const changeStreamDocumentUpdate = {
    ns: {
        db: 'metadata',
        coll: 'example-bucket',
    },
    documentKey: {
        _id: 'example-key',
    },
    operationType: 'update',
    clusterTime: {
        $timestamp: {
            t: 1701270357,
            i: 1,
        },
    },    updateDescription: {
        updatedFields: {
            value: {
                field: 'value',
            },
        },
    },
    fullDocument: null,
};

const getKafkaMessage = value => ({
    value: Buffer.from(value),
    timestamp: Date.now(),
    size: 2,
    topic: 'oplog-topic',
    offset: 1337,
    partition: 0,
    key: Buffer.from('key'),
});

describe('ListRecordStream', () => {
    let listRecordStream;
    beforeEach(() => {
        listRecordStream = new ListRecordStream(logger);
    });

    describe('_getType', () => {
        [
            {
                opType: 'insert',
                md: {},
                expected: 'put'
            },
            {
                opType: 'update',
                md: {
                    deleted: false,
                },
                expected: 'put'
            },
            {
                opType: 'update',
                md: {
                    deleted: true,
                },
                expected: 'delete'
            },
            {
                opType: 'replace',
                md: {},
                expected: 'put'
            },
            {
                opType: 'delete',
                md: undefined,
                expected: 'delete'
            },
            {
                opType: 'unsupported',
                md: undefined,
                expected: undefined
            }].forEach(params => {
                const { opType, md, expected } = params;
                it(`Should return correct operation type (${opType})`, done => {
                    const type = listRecordStream._getType(opType, md);
                    assert.strictEqual(type, expected);
                    return done();
                });
            });
    });

    describe('_transform', () => {
        it('Should correct format entry', done => {
            const kafkaMessage = getKafkaMessage(JSON.stringify(changeStreamDocument));
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
                        timestamp: '2023-11-29T15:05:57.000Z',
                    }],
                });
                return done();
            });
        });
        it('Should skip record if format is invalid', done => {
            const kafkaMessage = getKafkaMessage(JSON.stringify(changeStreamDocument));
            const InvalidKafkaMessage = getKafkaMessage('');
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
                        timestamp: '2023-11-29T15:05:57.000Z',
                    }],
                });
                return done();
            });
        });
    });

    describe('_getObjectMd', () => {
        [
            {
                it: 'insert',
                doc: changeStreamDocument,
                exp: changeStreamDocument.fullDocument.value,
            },
            {
                it: 'update',
                doc: changeStreamDocumentUpdate,
                exp: changeStreamDocumentUpdate.updateDescription.updatedFields.value,
            },
        ].forEach(params => {
            it(`Should return correct object metadata (${params.it})`, () => {
                const md = listRecordStream._getObjectMd(params.doc);
                assert.deepEqual(md, params.exp);
            });
        });
    });
});
