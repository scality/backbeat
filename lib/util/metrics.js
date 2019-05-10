/**
 * Get a pretty-print version of a duration in number of seconds, that
 * scales the duration units and gives up some precision for
 * readability.
 *
 * @param {number} durationSeconds - number of seconds to prettify

 * @return {string} human-readable form of durationSeconds, like
 * "5 hours" or "7 days".
 */
function prettyDuration(durationSeconds) {
    const _toPretty = (duration, unit) => `${Math.round(duration)} ${unit}`;

    let duration = durationSeconds;
    if (duration < 0) {
        return `invalid (${duration} seconds)`;
    }
    if (duration < 300) {
        return _toPretty(duration, 'seconds');
    }
    duration /= 60;
    if (duration < 300) {
        return _toPretty(duration, 'minutes');
    }
    duration /= 60;
    if (duration < 120) {
        return _toPretty(duration, 'hours');
    }
    duration /= 24;
    return _toPretty(duration, 'days');
}

module.exports = {
    prettyDuration,
};
