const assert = require('assert');

const ActionQueueEntry = require('../../../../lib/models/ActionQueueEntry');

// forge a new action received from Kafka
function mockActionFromKafka() {
    const entry = ActionQueueEntry.create('sendPigeon')
          .setAttribute('toCountry', 'Finland')
          .setAttribute('cost', 500)
          .addContext({
              purpose: 'secretMessage',
          })
          .setResultsTopic('ack-topic');
    const kafkaMessage = entry.toKafkaMessage();
    return ActionQueueEntry.createFromKafkaEntry({
        value: kafkaMessage,
    });
}

describe('ActionQueueEntry', () => {
    it('should create ActionQueueEntry with an action type and unique id',
    () => {
        const entry1 = ActionQueueEntry.create('sendPigeon');
        const entry2 = ActionQueueEntry.create('sendPigeon');
        assert.strictEqual(entry1.getActionType(), 'sendPigeon');
        assert.notStrictEqual(entry1.getActionId(), entry2.getActionId());
    });

    it('should return set attributes with getAttribute()', () => {
        const entry = ActionQueueEntry.create('sendPigeon')
              .setAttribute('toCountry', 'Finland')
              .setAttribute('cost', 500);
        assert.strictEqual(entry.getAttribute('toCountry'), 'Finland');
        assert.strictEqual(entry.getAttribute('cost'), 500);
        assert.strictEqual(entry.getAttribute('foo'), undefined);

        // nested attributes should work too
        entry.setAttribute('details.roundTrip', true);
        assert.strictEqual(entry.getAttribute('details.roundTrip'), true);
        assert.strictEqual(
            entry.getAttribute('details.roundTrip.foo', undefined));
        assert.deepStrictEqual(
            entry.getAttribute('details'), { roundTrip: true });
    });

    it('should set an error when retrieving an unset attribute and ' +
    '"required" option is set', () => {
        const entry = ActionQueueEntry.create('sendPigeon')
              .setAttribute('toCountry', 'Finland');
        assert.strictEqual(
            entry.getAttribute('cost', { required: true }), undefined);
        assert(entry.getError());
        assert.strictEqual(entry.getError().code, 400);
        assert.strictEqual(entry.getError().message, 'MissingParameter');
    });

    it('should return specified logged attributes from getLogInfo()', () => {
        const entry = ActionQueueEntry.create('sendPigeon')
              .setAttribute('toCountry', 'Finland')
              .setAttribute('cost', 500)
              .addLoggedAttributes({
                  pigeonDestCountry: 'toCountry',
                  pigeonCost: 'cost',
                  isRoundTrip: 'details.roundTrip',
              });
        assert.strictEqual(entry.getLogInfo().pigeonDestCountry, 'Finland');
        assert.strictEqual(entry.getLogInfo().pigeonCost, 500);
        // also some standard attributes
        assert.strictEqual(entry.getLogInfo().actionId, entry.getActionId());
        assert.strictEqual(entry.getLogInfo().actionType, 'sendPigeon');

        // because details.roundTrip is not set, it will not appear in logs
        assert(!Object.keys(entry.getLogInfo()).includes('isRoundTrip'));

        entry.setAttribute('details.roundTrip', true);
        assert.strictEqual(entry.getLogInfo().isRoundTrip, true);
    });

    it('should add some contextual info with addContext() that gets logged ' +
    'under "actionContext" field', () => {
        const entry = ActionQueueEntry.create('sendPigeon')
              .setAttribute('toCountry', 'Finland')
              .setAttribute('cost', 500)
              .addContext({
                  purpose: 'secretMessage',
              });
        assert.strictEqual(
            entry.getContextAttribute('purpose'), 'secretMessage');
        assert.deepStrictEqual(
            entry.getContext(), { purpose: 'secretMessage' });
        assert.strictEqual(
            entry.getContextAttribute('no.such.context'), undefined);
        assert.deepStrictEqual(
            entry.getLogInfo().actionContext, { purpose: 'secretMessage' });
    });

    it('should be able to set a results topic', () => {
        const entry = ActionQueueEntry.create('sendPigeon')
              .setResultsTopic('ack-topic');
        assert.strictEqual(entry.getResultsTopic(), 'ack-topic');
    });

    it('should handle action fetched with createFromKafkaEntry()', () => {
        const entry = ActionQueueEntry.create('sendPigeon')
              .setAttribute('toCountry', 'Finland')
              .setAttribute('cost', 500)
              .addContext({
                  purpose: 'secretMessage',
              })
              .setResultsTopic('ack-topic');
        const kafkaMessage = entry.toKafkaMessage();
        const fetchedEntry = ActionQueueEntry.createFromKafkaEntry({
            value: kafkaMessage,
        });
        assert.strictEqual(fetchedEntry.getActionId(), entry.getActionId());
        assert.strictEqual(fetchedEntry.getActionType(), 'sendPigeon');
        assert.strictEqual(fetchedEntry.getAttribute('toCountry'), 'Finland');
        assert.strictEqual(fetchedEntry.getAttribute('cost'), 500);
        assert.strictEqual(
            fetchedEntry.getContextAttribute('purpose'), 'secretMessage');
        assert.strictEqual(fetchedEntry.getResultsTopic(), 'ack-topic');
        assert.strictEqual(typeof fetchedEntry.getStartTime(), 'number');

        const badEntry1 = ActionQueueEntry.createFromKafkaEntry({
            value: 'badJson',
        });
        assert.notStrictEqual(badEntry1.error, undefined);
    });

    it('should be able to execute an action and set a success status', done => {
        const action1 = mockActionFromKafka();
        // action is being executed here, let's sleep a tiny amount of
        // time so to have different start and end times...
        setTimeout(() => {
            action1.setSuccess({ pigeonIsSick: false });
            const result1 = ActionQueueEntry.createFromKafkaEntry({
                value: action1.toKafkaMessage(),
            });
            assert.strictEqual(result1.getActionId(), action1.getActionId());
            assert.strictEqual(result1.getStatus(), 'success');
            assert.deepStrictEqual(
                result1.getResults(), { pigeonIsSick: false });
            assert.strictEqual(typeof result1.getStartTime(), 'number');
            assert.strictEqual(typeof result1.getEndTime(), 'number');
            // result shall hold the same timestamps than the original
            // action recorded
            assert.strictEqual(action1.getStartTime(), result1.getStartTime());
            assert.strictEqual(action1.getEndTime(), result1.getEndTime());
            assert.strictEqual(typeof result1.getElapsedMs(), 'number');
            assert.strictEqual(
                result1.getElapsedMs(), result1.getLogInfo().elapsed_ms);

            const action2 = mockActionFromKafka();
            // action is being executed here...

            action2.setEnd(null, { pigeonIsSick: false });
            const result2 = ActionQueueEntry.createFromKafkaEntry({
                value: action2.toKafkaMessage(),
            });
            assert.strictEqual(result2.getActionId(), action2.getActionId());
            assert.strictEqual(result2.getStatus(), 'success');
            assert.deepStrictEqual(
                result2.getResults(), { pigeonIsSick: false });
            done();
        }, 10);
    });

    it('should be able to execute an action and set an error status', () => {
        const action1 = mockActionFromKafka();
        // action is being executed here...

        action1.setError(new Error('pigeon is lost'));
        const result1 = ActionQueueEntry.createFromKafkaEntry({
            value: action1.toKafkaMessage(),
        });
        assert.strictEqual(result1.getActionId(), action1.getActionId());
        assert.strictEqual(result1.getStatus(), 'error');
        assert.deepStrictEqual(result1.getError(), {
            message: 'pigeon is lost',
        });

        const action2 = mockActionFromKafka();
        // action is being executed here...

        action2.setEnd(new Error('pigeon is lost'));
        const result2 = ActionQueueEntry.createFromKafkaEntry({
            value: action2.toKafkaMessage(),
        });
        assert.strictEqual(result2.getActionId(), action2.getActionId());
        assert.strictEqual(result2.getStatus(), 'error');
        assert.deepStrictEqual(result2.getError(), {
            message: 'pigeon is lost',
        });
    });
});
