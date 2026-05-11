/**
 * Parse JSON without throwing an exception
 * @param {String} str - The stringified json
 * @return {Object} - Object with any error and JSON parse value
 */
function safeJsonParse(str) {
    let result = null;
    try {
        result = JSON.parse(str);
    } catch (err) {
        return { error: err };
    }
    return { error: null, result };
}

module.exports = safeJsonParse;
