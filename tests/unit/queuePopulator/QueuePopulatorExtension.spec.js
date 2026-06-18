'use strict';

const assert = require('assert');

const QueuePopulatorExtension =
    require('../../../lib/queuePopulator/QueuePopulatorExtension');
const fakeLogger = require('../../utils/fakeLogger');

describe('QueuePopulatorExtension::publish', () => {
    let ext;
    beforeEach(() => {
        ext = new QueuePopulatorExtension({ config: {}, logger: fakeLogger });
    });

    it('attaches kafka headers when provided', () => {
        const batch = {};
        const headers = [{ traceparent: '00-abc-def-01' }];
        ext.publish('topic', 'key', 'message', batch, headers);
        assert.deepStrictEqual(batch.topic[0].headers, headers);
    });

    it('omits the headers field when none are provided', () => {
        const batch = {};
        ext.publish('topic', 'key', 'message', batch);
        assert.strictEqual('headers' in batch.topic[0], false);
    });
});
