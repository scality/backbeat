/**
 * Builds a change stream resume token, serialized as mongo-kafka commits it
 * in its source offsets, whose cluster time is (t, i)
 * @param {number} t cluster time seconds
 * @param {number} [i] cluster time increment
 * @returns {string} resume token as JSON
 */
function resumeTokenAt(t, i = 0) {
    return JSON.stringify({ _data: `82${t.toString(16).padStart(8, '0')}${i.toString(16).padStart(8, '0')}04` });
}

module.exports = { resumeTokenAt };
