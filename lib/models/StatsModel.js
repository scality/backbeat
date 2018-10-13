const async = require('async');

const { StatsClient } = require('arsenal').metrics;

/**
 * @class StatsModel
 *
 * @classdesc Extend and overwrite how timestamps are normalized by minutes
 * rather than by seconds
 */
class StatsModel extends StatsClient {

    /**
    * Utility method to convert 2d array rows to columns, and vice versa
    * See also: https://docs.ruby-lang.org/en/2.0.0/Array.html#method-i-zip
    * @param {array} arrays - 2d array of integers
    * @return {array} converted array
    */
    _zip(arrays) {
        return arrays[0].map((_, i) => arrays.map(a => a[i]));
    }

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

    /**
    * wrapper on `getStats` that handles a list of keys
    * override the method to reduce the returned 2d array from `_getCount`
    * @param {object} log - Werelogs request logger
    * @param {array} ids - service identifiers
    * @param {callback} cb - callback to call with the err/result
    * @return {undefined}
    */
    getAllStats(log, ids, cb) {
        if (!this._redis) {
            return cb(null, {});
        }

        const statsRes = {
            'requests': [],
            '500s': [],
            'sampleDuration': this._expiry,
        };
        const requests = [];
        const errors = [];

        // for now set concurrency to default of 10
        return async.eachLimit(ids, 10, (id, done) => {
            this.getStats(log, id, (err, res) => {
                if (err) {
                    return done(err);
                }
                requests.push(res.requests);
                errors.push(res['500s']);
                return done();
            });
        }, error => {
            if (error) {
                log.error('error getting stats', {
                    error,
                    method: 'StatsModel.getAllStats',
                });
                return cb(null, statsRes);
            }

            if (requests.length) {
                statsRes.requests = this._zip(requests).map(arr =>
                    arr.reduce((acc, i) => acc + i, 0));
            }
            if (errors.length) {
                statsRes['500s'] = this._zip(errors).map(arr =>
                    arr.reduce((acc, i) => acc + i, 0));
            }
            return cb(null, statsRes);
        });
    }
}

module.exports = StatsModel;
