const assert = require('assert');

const jsutil = require('arsenal').jsutil;

const BackbeatConsumer = require('../../lib/BackbeatConsumer');

class BackbeatTestConsumer extends BackbeatConsumer {
    constructor(config) {
        super(Object.assign({}, config,
                            { queueProcessor: function dummy() {},
                              bootstrap: true }));
        // hook queue processor function
        this._queueProcessor = this._processMessage.bind(this);
        this._expectVars = null;
    }

    _processMessage(message, done) {
        function _matchMessage(expectedMsg) {
            if (expectedMsg.key !== undefined) {
                assert.deepStrictEqual(
                    message.key.toString(), expectedMsg.key,
                    `unexpected message key ${message.key}, ` +
                        `expected ${expectedMsg.key}`);
            }
            if (expectedMsg.value !== undefined) {
                const isExpectedObject = typeof expectedMsg.value === 'object';
                const parsedMsg = isExpectedObject ?
                    JSON.parse(message.value) :
                    message.value.toString();
                const expectedValue = isExpectedObject ?
                    JSON.parse(JSON.stringify(expectedMsg.value)) :
                    expectedMsg.value;
                if (isExpectedObject &&
                    expectedValue.contextInfo?.reqId === 'test-request-id') {
                    // RequestId is generated randomly, we can't compare it: just check that it is
                    // present
                    assert(parsedMsg.contextInfo?.reqId, 'expected contextInfo.reqId field');
                    expectedValue.contextInfo.reqId = parsedMsg.contextInfo.reqId;
                    if (expectedValue.contextInfo?.conductorScanId === 'test-scan-id') {
                        assert(parsedMsg.contextInfo?.conductorScanId,
                            'expected contextInfo.conductorScanId field');
                        expectedValue.contextInfo.conductorScanId =
                            parsedMsg.contextInfo.conductorScanId;
                    }
                    if (expectedValue.contextInfo?.conductorScanStartTimestamp === 0) {
                        assert(parsedMsg.contextInfo?.conductorScanStartTimestamp,
                            'expected contextInfo.conductorScanStartTimestamp field');
                        expectedValue.contextInfo.conductorScanStartTimestamp =
                            parsedMsg.contextInfo.conductorScanStartTimestamp;
                    }
                }
                assert.deepStrictEqual(
                    parsedMsg, expectedValue,
                    `unexpected message value ${parsedMsg}, ` +
                        `expected ${expectedValue}`);
            }
        }

        const v = this._expectVars;
        if (v) {
            if (v.ordered) {
                const expectedMsg = v.remainingMsgList.pop();
                try {
                    _matchMessage(expectedMsg);
                } catch (err) {
                    v.cbOnce(err);
                }
            } else {
                const newRemainingList = [];
                let matched = false;
                v.remainingMsgList.forEach(expectedMsg => {
                    if (matched) {
                        newRemainingList.push(expectedMsg);
                    } else {
                        try {
                            _matchMessage(expectedMsg);
                            matched = true;
                        } catch {
                            newRemainingList.push(expectedMsg);
                        }
                    }
                });
                v.remainingMsgList = newRemainingList;
                try {
                    assert(matched, `unexpected message: ${message.value}`);
                } catch (err) {
                    return v.cbOnce(err);
                }
            }
            if (v.remainingMsgList.length === 0) {
                v.cbOnce();
            }
        }
        return process.nextTick(done);
    }

    _expectMessages(messages, timeout, ordered, cb) {
        const cbOnce = jsutil.once(err => {
            this._expectVars = null;
            return cb(err);
        });
        setTimeout(() => {
            const v = this._expectVars;
            if (v) {
                try {
                    assert.deepStrictEqual(
                        [], v.remainingMsgList,
                        `missing messages after ${timeout}ms timeout`);
                } catch (err) {
                    cbOnce(err);
                }
            }
        }, timeout);

        const v = {
            cbOnce,
            remainingMsgList: Array.from(messages).reverse(),
            ordered,
        };
        this._expectVars = v;
    }

    /**
     * Consume messages from the topic and and wait until all messages
     * in {@link messages} have been received exactly once
     *
     * All messages are expected to be received in order, and exactly
     * once: an error will be raised otherwise.
     *
     * @param {Array} messages - ordered list of messages to expect:
     *   each item may have a key and/or a value element to check
     *   against the received message.
     * @param {Number} timeout - number of milliseconds to wait until
     *   giving up with an error
     * @param {function} cb - cb(err): callback called with no error
     *   argument when either the last message has just been received
     *   and all others have already been received in order, or with
     *   an error in other cases.
     * @return {undefined}
     */
    expectOrderedMessages(messages, timeout, cb) {
        this._expectMessages(messages, timeout, true, cb);
    }

    /**
     * Consume messages from the topic and and wait until all messages
     * in {@link messages} have been received exactly once.
     *
     * Messages can be received in any order.
     *
     * @param {Array} messages - unordered list of messages to expect:
     *   each item may have a key and/or a value element to check
     *   against the received message.
     * @param {Number} timeout - number of milliseconds to wait until
     *   giving up with an error
     * @param {function} cb - cb(err): callback called with no error
     *   argument when either the last message has just been received
     *   and all others have already been received, or with an error
     *   in other cases.
     * @return {undefined}
     */
    expectUnorderedMessages(messages, timeout, cb) {
        this._expectMessages(messages, timeout, false, cb);
    }
}

module.exports = BackbeatTestConsumer;
