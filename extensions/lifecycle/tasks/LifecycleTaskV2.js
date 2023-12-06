'use strict'; // eslint-disable-line

const async = require('async');
const { errors } = require('arsenal');

const LifecycleTask = require('./LifecycleTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { rulesToParams } = require('../util/rules');
const { lifecycleListing: { NON_CURRENT_TYPE, CURRENT_TYPE, ORPHAN_DM_TYPE } } = require('../../../lib/constants');
// concurrency mainly used in async calls
const CONCURRENCY_DEFAULT = 10;

class LifecycleTaskV2 extends LifecycleTask {
    /**
     * NOTE: Among other methods, LifecycleTaskV2 inherits the processBucketEntry method from LifecycleTask.
     */

    /**
     * Skips kafka entry
     * @param {object} bucketData - bucket data from bucketTasksTopic
     * @param {Logger.newRequestLogger} log - logger object
     * @return {boolean} true if skipped, false otherwise.
     */
    _skipEntry(bucketData, log) {
        const { details } = bucketData;
        // NOTE: keyMarker is not specified here because it is also used for MPU.
        if ((details.versionIdMarker || details.marker) && !details.listType) {
            log.debug('skip entry generated by the old lifecycle task', {
                method: 'LifecycleTaskV2._skipEntry',
                bucketData,
            });
            return true;
        }
        return false;
    }

    /**
     * Handles remaning listing asynchronously
     * @param {array} remainings - array of { prefix, listType, beforeDate }
     * @param {object} bucketData - bucket data
     * @param {Logger.newRequestLogger} log - logger object
     * @return {undefined}
     */
    _handleRemainingListings(remainings, bucketData, log) {
        if (remainings && remainings.length) {
            remainings.forEach(l => {
                const {
                    prefix,
                    listType,
                    beforeDate,
                    storageClass,
                } = l;

                const entry = Object.assign({}, bucketData, {
                    contextInfo: { requestId: log.getSerializedUids() },
                    details: { beforeDate, prefix, listType, storageClass },
                });

                this._sendBucketEntry(entry, err => {
                    if (!err) {
                        log.debug(
                            'sent kafka entry for bucket consumption', {
                                method: 'LifecycleTaskV2._getVersionList',
                            });
                    }
                });
            });
        }
    }

    /**
     * Handles non-versioned objects
     * @param {object} bucketData - bucket data
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {number} nbRetries - Number of time the process has been retried
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _getObjectList(bucketData, bucketLCRules, nbRetries, log, done) {
        const currentDate = new Date();

        const { transitionOneDayEarlier, expireOneDayEarlier, timeProgressionFactor } = this.lcOptions;
        const ruleOptions = {
            transitionOneDayEarlier,
            expireOneDayEarlier,
            timeProgressionFactor,
        };

        const { params, listType, remainings } =
            rulesToParams('Disabled', currentDate, bucketLCRules, bucketData, ruleOptions);
        // If params is undefined listings can be skipped.
        // Undefined params can happen when lifecycle configuration rule is disabled for example
        if (!params) {
            log.debug('no appropriate listing found', {
                method: 'LifecycleTaskV2._getObjectList',
                bucketLCRules,
                currentDate,
                bucketData,
            });
            return process.nextTick(done);
        }

        // re-queue remaining listings only once
        if (nbRetries === 0) {
            this._handleRemainingListings(remainings, bucketData, log);
        }

        return this.backbeatMetadataProxy.listLifecycle(listType, params, log,
        (err, contents, isTruncated, markerInfo) => {
            if (err) {
                return done(err);
            }

            // re-queue truncated listing only once.
            if (isTruncated && nbRetries === 0) {
                const entry = Object.assign({}, bucketData, {
                    contextInfo: { requestId: log.getSerializedUids() },
                    details: {
                        beforeDate: params.BeforeDate,
                        prefix: params.Prefix,
                        storageClass: params.ExcludedDataStoreName,
                        listType,
                        ...markerInfo
                    },
                });

                this._sendBucketEntry(entry, err => {
                    if (!err) {
                        log.debug(
                            'sent kafka entry for bucket consumption', {
                                method: 'LifecycleTaskV2._getObjectList',
                            });
                    }
                });
            }
            return this._compareRulesToList(bucketData, bucketLCRules,
                contents, log, done);
        });
    }

    /**
     * Handles versioned objects
     * @param {object} bucketData - bucket data
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {string} versioningStatus - bucket version status: Enabled, Disabled or Suspended
     * @param {number} nbRetries - Number of time the process has been retried
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _getObjectVersions(bucketData, bucketLCRules, versioningStatus, nbRetries, log, done) {
        const currentDate = new Date();

        const { transitionOneDayEarlier, expireOneDayEarlier, timeProgressionFactor } = this.lcOptions;
        const ruleOptions = {
            transitionOneDayEarlier,
            expireOneDayEarlier,
            timeProgressionFactor,
        };

        const { params, listType, remainings } =
            rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, ruleOptions);
        // If params is undefined listings can be skipped.
        // Undefined params can happen when lifecycle configuration rule is disabled for example.
        if (!params) {
            log.debug('no appropriate listing found', {
                method: 'LifecycleTaskV2._getObjectList',
                bucketLCRules,
                currentDate,
                bucketData,
            });
            return process.nextTick(done);
        }

        // re-queue remaining listings only once
        if (nbRetries === 0) {
            this._handleRemainingListings(remainings, bucketData, log);
        }

        return this.backbeatMetadataProxy.listLifecycle(listType, params, log,
        (err, contents, isTruncated, markerInfo) => {
            if (err) {
                return done(err);
            }

            // create Set of unique keys not matching the next marker to
            // indicate the object level entries to be cleared at the end
            // of the processing step
            const uniqueObjectKeysNotNextMarker = new Set();
            if (markerInfo.keyMarker) {
                contents.forEach(v => {
                    if (v.Key !== markerInfo.keyMarker) {
                        uniqueObjectKeysNotNextMarker.add(v.Key);
                    }
                });
            }

            // re-queue truncated listing only once.
            if (isTruncated && nbRetries === 0) {
                const entry = Object.assign({}, bucketData, {
                    contextInfo: { requestId: log.getSerializedUids() },
                    details: {
                        beforeDate: params.BeforeDate,
                        prefix: params.Prefix,
                        storageClass: params.ExcludedDataStoreName,
                        listType,
                        ...markerInfo,
                    },
                });

                this._sendBucketEntry(entry, err => {
                    if (!err) {
                        log.debug(
                            'sent kafka entry for bucket consumption', {
                                method: 'LifecycleTaskV2._getObjectList',
                            });
                    }
                });
            }
            return this._compareRulesToList(bucketData, bucketLCRules,
                contents, log, err => {
                    if (err) {
                        return done(err);
                    }

                    if (!isTruncated) {
                        // end of bucket listing
                        // clear bucket level entry and all object entries
                        this._ncvHeapBucketClear(bucketData.target.bucket);
                    } else {
                        // clear object level entries that have been processed
                        this._ncvHeapObjectsClear(
                            bucketData.target.bucket,
                            uniqueObjectKeysNotNextMarker
                        );
                    }

                    return done();
                });
        });
    }

    /**
     * For each key, based on the lifecycle rules, evaluates if object is eligible.
     * @param {object} bucketData - bucket data
     * @param {object} bucketData.target - target bucket info
     * @param {string} bucketData.target.bucket - bucket name
     * @param {string} bucketData.target.owner - owner id
     * @param {string} [bucketData.prefix] - prefix
     * @param {string} [bucketData.details.keyMarker] - next key marker for versioned buckets
     * @param {string} [bucketData.details.versionIdMarker] - next version id marker for versioned buckets
     * @param {string} [bucketData.details.marker] - next marker for non-versioned buckets
     * @param {array} lcRules - array of bucket lifecycle rules
     * @param {array} contents - array of object or object version
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error)
     * @return {undefined}
     */
    _compareRulesToList(bucketData, lcRules, contents, log, done) {
        if (!contents.length) {
            return done();
        }
        return async.eachLimit(contents, CONCURRENCY_DEFAULT, (obj, cb) => {
            const applicableRules = this._getRules(bucketData, lcRules, obj);
            return this._retryEntry({
                logFields: {
                    key: obj.Key,
                    versionId: obj.VersionId,
                    staleDate: obj.staleDate,
                },
                log,
                actionFunc: done => this._compare(bucketData, obj, applicableRules, log, done),
            }, cb);
        }, done);
    }

    /**
     * For a given object, returns the rules that apply the earliest.
     * @param {object} bucketData - bucket data
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {object} object - object or object version
     * @return {undefined}
     */
    _getRules(bucketData, bucketLCRules, object) {
        const objTags = { TagSet: object.TagSet };
        const filteredRules = this._lifecycleUtils.filterRules(bucketLCRules, object, objTags);
        return this._lifecycleUtils.getApplicableRules(filteredRules, object);
    }

    /**
     * Evaluates if object is eligible.
     * @param {object} bucketData - bucket data
     * @param {object} obj - object or object version
     * @param {array} rules - array of bucket lifecycle rules
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} cb - callback(error)
     * @return {undefined}
     */
    _compare(bucketData, obj, rules, log, cb) {
        if (obj.ListType === CURRENT_TYPE) {
            return this._compareCurrent(bucketData, obj, rules, log, cb);
        }

        if (obj.ListType === NON_CURRENT_TYPE) {
            return this._compareNonCurrent(bucketData, obj, rules, log, cb);
        }

        if (obj.ListType === ORPHAN_DM_TYPE) {
            this._checkAndApplyEODMRule(bucketData, obj, rules, log);
            return process.nextTick(cb);
        }

        // This should never happen
        log.error('object listType is missing', {
            bucket: bucketData.target.bucket,
            key: obj.Key,
            listType: obj.ListType,

        });

        const errMsg = 'an implementation error occurred: listType is missing';
        return process.nextTick(() => cb(errors.InternalError.customizeDescription(errMsg)));
    }

    /**
     * Compare a non versioned object or a current version to most applicable rules
     * @param {object} bucketData - bucket data
     * @param {object} obj - single object from `listObjects`
     * @param {string} obj.LastModified - last modified date of object
     * @param {object} rules - most applicable rules from `_getApplicableRules`
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} cb - callback(error)
     * @return {undefined}
     */
    _compareCurrent(bucketData, obj, rules, log, cb) {
        if (rules.Expiration &&
            this._checkAndApplyExpirationRule(bucketData, obj, rules, log)) {
            return process.nextTick(cb);
        }
        if (rules.Transition) {
            return this._applyTransitionRule({
                owner: bucketData.target.owner,
                accountId: bucketData.target.accountId,
                bucket: bucketData.target.bucket,
                objectKey: obj.Key,
                versionId: obj.VersionId,
                eTag: obj.ETag,
                lastModified: obj.LastModified,
                site: rules.Transition.StorageClass,
                transitionTime: this._lifecycleDateTime.getTransitionTimestamp(
                    rules.Transition, obj.LastModified),
            }, log, cb);
        }

        return process.nextTick(cb);
    }

    /**
     * Compare a non-current versioned object to most applicable rules
     * @param {object} bucketData - bucket data
     * @param {object} obj - single object from `listObjects`
     * @param {string} obj.LastModified - last modified date of object
     * @param {object} rules - most applicable rules from `_getApplicableRules`
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} cb - callback(error)
     * @return {undefined}
     */
    _compareNonCurrent(bucketData, obj, rules, log, cb) {
        if (!obj.staleDate) {
            // NOTE: this should never happen. Logging here for debug purposes
            log.error('missing staleDate on the version', {
                method: 'LifecycleTaskV2._compareNonCurrent',
                bucket: bucketData.target.bucket,
                versionId: obj.VersionId,
            });
            const errMsg = 'an implementation error occurred: when comparing ' +
                'lifecycle rules on a version, stale date was missing';
            return process.nextTick(() => cb(errors.InternalError.customizeDescription(errMsg)));
        }

        if (rules.NoncurrentVersionExpiration &&
            this._checkAndApplyNCVExpirationRule(bucketData, obj, rules, log)) {
            return process.nextTick(cb);
        }

        if (rules.NoncurrentVersionTransition) {
            return this._checkAndApplyNCVTransitionRule(bucketData, obj, rules, log, cb);
        }

        return process.nextTick(cb);
    }

    /**
     * Helper method for Expiration.ExpiredObjectDeleteMarker rule
     * Check if ExpiredObjectDeleteMarker rule applies to the `IsLatest` delete
     * marker
     * @param {object} bucketData - bucket data
     * @param {object} deleteMarker - single non-current version
     * @param {string} deleteMarker.Key - key name
     * @param {string} deleteMarker.VersionId - version id
     * @param {object} rules - most applicable rules from `_getApplicableRules`
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error)
     * @return {undefined}
     */
    _checkAndApplyEODMRule(bucketData, deleteMarker, rules, log) {
        const daysSinceInitiated = this._lifecycleDateTime.findDaysSince(
            new Date(deleteMarker.LastModified)
        );

        const eodm = rules.Expiration &&
            rules.Expiration.ExpiredObjectDeleteMarker;

        const applicableExpRule = rules.Expiration && (
            (rules.Expiration.Days !== undefined &&
                daysSinceInitiated >= rules.Expiration.Days) ||
            (rules.Expiration.Date !== undefined &&
                rules.Expiration.Date < Date.now()) ||
            eodm === true
        );

        if (applicableExpRule) {
            const entry = ActionQueueEntry.create('deleteObject')
                .addContext({
                    origin: 'lifecycle',
                    ruleType: 'expiration',
                    reqId: log.getSerializedUids(),
                })
                .setAttribute('target.owner', bucketData.target.owner)
                .setAttribute('target.bucket', bucketData.target.bucket)
                .setAttribute('target.key', deleteMarker.Key)
                .setAttribute('target.accountId', bucketData.target.accountId)
                .setAttribute('target.version', deleteMarker.VersionId)
                .setAttribute('transitionTime',
                    this._lifecycleDateTime.getTransitionTimestamp(
                        applicableExpRule, deleteMarker.LastModified)
                );
            this._sendObjectAction(entry, err => {
                if (!err) {
                    log.debug('sent object entry for consumption',
                    Object.assign({
                        method: 'LifecycleTaskV2._checkAndApplyEODMRule',
                    }, entry.getLogInfo()));
                }
            });
        }
    }
}

module.exports = LifecycleTaskV2;
