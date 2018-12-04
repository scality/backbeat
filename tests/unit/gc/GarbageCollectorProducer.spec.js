'use strict'; // eslint-disable-line

const assert = require('assert');

const GarbageCollectorProducer =
      require('../../../extensions/gc/GarbageCollectorProducer');

class KafkaProducerMock {
    constructor() {
        this._messageExpected = null;
    }

    setExpectedMessage(message) {
        this._expectedMessage = message;
    }

    hasProcessedExpectedMessage() {
        return this._expectedMessage === null;
    }

    send(messages, cb) {
        if (this._expectedMessage) {
            assert.strictEqual(messages.length, 1);
            assert.deepStrictEqual(this._expectedMessage,
                                   JSON.parse(messages[0].message));
            this._expectedMessage = null;
        }
        return process.nextTick(cb);
    }
}

describe('garbage collector producer', () => {
    let gcProducer;
    const kafkaProducerMock = new KafkaProducerMock();

    before(() => {
        gcProducer = new GarbageCollectorProducer();
        gcProducer._producer = kafkaProducerMock;
    });
    [{
        testDesc: 'with no dataStoreVersionId',
        dataLocations: [{
            key: 'foo',
            dataStoreName: 'ds',
            size: 10,
        }],
    }, {
        testDesc: 'with a dataStoreVersionId',
        dataLocations: [{
            key: 'foo',
            dataStoreName: 'ds',
            size: 10,
            dataStoreVersionId: 'someversion',
        }],
    }].forEach(testSpec => {
        it(`should send a valid GC message to kafka ${testSpec.testDesc}`,
        done => {
            kafkaProducerMock.setExpectedMessage({
                action: 'deleteData',
                target: {
                    locations: testSpec.dataLocations,
                },
            });
            gcProducer.publishDeleteDataEntry(testSpec.dataLocations, err => {
                assert.ifError(err);
                assert(kafkaProducerMock.hasProcessedExpectedMessage());
                done();
            });
        });
    });
});
