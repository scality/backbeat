const { StatsClient } = require('arsenal').metrics;

/**
 * @class StatsModel
 *
 * @classdesc Extend and overwrite how timestamps are normalized by minutes
 * rather than by seconds
 */
class StatsModel extends StatsClient {

    /**
    * normalize to the nearest interval
    * @param {object} d - Date instance
    * @return {number} timestamp - normalized to the nearest interval
    */
    _normalizeTimestamp(d) {
        const m = d.getMinutes();
        return d.setMinutes(m - m % (Math.floor(this._interval / 60)), 0, 0);
    }

    /**
    * override the method to get the result as an array of integers separated
    * by each interval
    * typical input looks like [[null, '1'], [null, '2'], [null, null]...]
    * @param {array} arr - each index contains the result of each batch command
    *   where index 0 signifies the error and index 1 contains the result
    * @return {array} array of integers, ordered from most recent interval to
    *   oldest interval
    */
    _getCount(arr) {
        return arr.reduce((store, i) => {
            let num = parseInt(i[1], 10);
            num = Number.isNaN(num) ? 0 : num;
            store.push(num);
            return store;
        }, []);
    }
}

module.exports = StatsModel;
