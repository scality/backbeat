const safeJsonParse = require('../../../lib/util/safeJsonParse');

// KeyString type byte of a BSON timestamp, which leads every change stream
// resume token: the token starts with the event's cluster time
const keyStringTimestampType = '82';
const resumeTokenRegex = new RegExp(`^${keyStringTimestampType}[0-9a-fA-F]{16}`);

/**
 * Extracts the cluster time of a change stream resume token
 * @param {Object} token resume token, `{ _data: '82...' }`
 * @returns {Object|null} `{ t, i }`, or null if not a resume token
 */
function timestampFromResumeToken(token) {
    const data = token?._data;
    if (typeof data !== 'string' || !resumeTokenRegex.test(data)) {
        return null;
    }
    const buf = Buffer.from(data.slice(2, 18), 'hex');
    return { t: buf.readUInt32BE(0), i: buf.readUInt32BE(4) };
}

/**
 * Extracts the cluster time of a mongo-kafka source offset, whose `_id`
 * holds the resume token serialized as JSON
 * @param {Object} offset source offset, as committed by the connector
 * @returns {Object|null} `{ t, i }`, or null
 */
function timestampFromSourceOffset(offset) {
    if (typeof offset?._id !== 'string') {
        return null;
    }
    const { error, result } = safeJsonParse(offset._id);
    return error ? null : timestampFromResumeToken(result);
}

/**
 * Parses a `startup.mode.timestamp.start.at.operation.time` value: BSON
 * timestamp as extended JSON, epoch seconds, or ISO-8601 date (sub-second
 * precision is dropped, as the connector does)
 * @param {string} value configured value
 * @returns {Object|null} `{ t, i }`, or null
 */
function timestampFromStartupTime(value) {
    if (typeof value !== 'string' || !value) {
        return null;
    }
    const { result } = safeJsonParse(value);
    const ts = result?.$timestamp;
    if (Number.isInteger(ts?.t) && Number.isInteger(ts?.i)) {
        return { t: ts.t, i: ts.i };
    }
    if (/^\d+$/.test(value)) {
        return { t: Number(value), i: 0 };
    }
    const ms = Date.parse(value);
    return Number.isNaN(ms) ? null : { t: Math.floor(ms / 1000), i: 0 };
}

/**
 * Formats a timestamp for `startup.mode.timestamp.start.at.operation.time`,
 * keeping the increment
 * @param {Object} ts `{ t, i }`
 * @returns {string} extended JSON timestamp
 */
function formatStartupTime(ts) {
    return JSON.stringify({ $timestamp: { t: ts.t, i: ts.i } });
}

/**
 * @param {Object} a `{ t, i }`
 * @param {Object} b `{ t, i }`
 * @returns {number} negative if a < b, positive if a > b, 0 if equal
 */
function compareTimestamps(a, b) {
    return a.t - b.t || a.i - b.i;
}

/**
 * @param {Object[]} timestamps list of `{ t, i }`
 * @returns {Object|null} earliest timestamp, or null if the list is empty
 */
function minTimestamp(timestamps) {
    return timestamps.reduce(
        (min, ts) => (min === null || compareTimestamps(ts, min) < 0 ? ts : min), null);
}

module.exports = {
    timestampFromResumeToken,
    timestampFromSourceOffset,
    timestampFromStartupTime,
    formatStartupTime,
    compareTimestamps,
    minTimestamp,
};
