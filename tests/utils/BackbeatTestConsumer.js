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
                const parsedMsg = typeof expectedMsg.value === 'object' ?
                          JSON.parse(message.value) :
                          message.value.toString();
                if (typeof expectedMsg.value === 'object' &&
                    expectedMsg.value.contextInfo?.reqId === 'test-request-id') {
                    // RequestId is generated randomly, we can't compare it: just check that it is
                    // present
                    assert(parsedMsg.contextInfo?.reqId, 'expected contextInfo.reqId field');
                    parsedMsg.contextInfo.reqId = expectedMsg.value.contextInfo?.reqId;
                }
                assert.deepStrictEqual(
                    parsedMsg, expectedMsg.value,
                    `unexpected message value ${parsedMsg}, ` +
                        `expected ${expectedMsg.value}`);
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
                        } catch (err) {
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
