'use strict'; // eslint-disable-line

const async = require('async');
const { errors } = require('arsenal');

const LifecycleTask = require('./LifecycleTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { rulesToParams } = require('../util/rules');
const { lifecycleListing: { NON_CURRENT_TYPE, CURRENT_TYPE, ORPHAN_DM_TYPE } } = require('../../../lib/constants');

// concurrency mainly used in async calls
const CONCURRENCY_DEFAULT = 10;

const expireOneDayEarlier = process.env.EXPIRE_ONE_DAY_EARLIER === 'true';
const transitionOneDayEarlier = process.env.TRANSITION_ONE_DAY_EARLIER === 'true';
const ruleOptions = {
    expireOneDayEarlier,
    transitionOneDayEarlier,
};

function isLifecycleUser(canonicalID) {
    const canonicalIDArray = canonicalID.split('/');
    const serviceName = canonicalIDArray[canonicalIDArray.length - 1];
    return serviceName === 'lifecycle';
}

class LifecycleTaskV2 extends LifecycleTask {

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
                const prefix = l.prefix;
                const listType = l.listType;
                const beforeDate = l.beforeDate;

                const entry = Object.assign({}, bucketData, {
                    details: { beforeDate, prefix, listType },
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

        // handle remaining listings only once
        if (nbRetries === 0) {
            this._handleRemainingListings(remainings, bucketData, log);
        }

        // nb of listings is limited per bucket with reducer.
        return this.backbeatMetadataProxy.listLifecycle(listType, params, log,
        (err, contents, isTruncated, markerInfo) => {
            if (err) {
                return done(err);
            }

            // re-queue to Kafka topic bucketTasksTopic
            // with bucket name and `data.marker` only once.
            if (isTruncated && nbRetries === 0) {
                const entry = Object.assign({}, bucketData, {
                    details: {
                        beforeDate: params.BeforeDate,
                        prefix: params.Prefix,
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
            // TODO: compare objects to Rules
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

        // handle remaining listings only once
        if (nbRetries === 0) {
            this._handleRemainingListings(remainings, bucketData, log);
        }

        return this.backbeatMetadataProxy.listLifecycle(listType, params, log,
        (err, contents, isTruncated, markerInfo) => {
            if (err) {
                return done(err);
            }

            // re-queue to Kafka topic bucketTasksTopic
            // with bucket name and `data.marker` only once.
            if (isTruncated && nbRetries === 0) {
                const entry = Object.assign({}, bucketData, {
                    details: {
                        beforeDate: params.BeforeDate,
                        prefix: params.Prefix,
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
            // TODO: compare objects to Rules
            return this._compareRulesToList(bucketData, bucketLCRules,
                contents, log, done);
        });
    }

    _compareRulesToList(bucketData, lcRules, contents, log, done) {
        if (!contents.length) {
            return done();
        }
        return async.eachLimit(contents, CONCURRENCY_DEFAULT, (obj, cb) => {
            const applicableRules = this._getRules(bucketData, lcRules, obj);
            return this._compare(bucketData, obj,
                    applicableRules, log, cb);
        }, done);
    }

    /**
     * Compare a non versioned object or a current version to most applicable rules
     * @param {object} bucketData - bucket data
     * @param {object} obj - single object from `listObjects`
     * @param {string} obj.LastModified - last modified date of object
     * @param {object} rules - most applicable rules from `_getApplicableRules`
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    _compareCurrent(bucketData, obj, rules, log, cb) {
        console.log('rules!!!', rules);
        if (rules.Expiration) {
            this._checkAndApplyExpirationRule(bucketData, obj, rules,
                log);

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
            }, log, cb);
        }

        return process.nextTick(cb);
    }

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

        if (rules.NoncurrentVersionExpiration) {
            this._checkAndApplyNCVExpirationRule(bucketData, obj, rules, log);
            return process.nextTick(cb);
        }

        if (rules.NoncurrentVersionTransition) {
            return this._checkAndApplyNCVTransitionRule(bucketData, obj, rules, log, cb);
        }

        return process.nextTick(cb);
    }

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
            eodm !== false
        );
        // TODO: check why this check exists
        const validLifecycleUserCase = (
            isLifecycleUser(deleteMarker.Owner.ID) &&
            eodm !== false
        );

        // if there are no other versions with the same Key as this DM,
        // if a valid Expiration rule exists or if the DM was created
        // by a lifecycle service account and eodm rule is not
        // explicitly set to false, apply and permanently delete this DM
        if (applicableExpRule || validLifecycleUserCase) {
            const entry = ActionQueueEntry.create('deleteObject')
                .addContext({
                    origin: 'lifecycle',
                    ruleType: 'expiration',
                    reqId: log.getSerializedUids(),
                })
                .setAttribute('target.owner', bucketData.target.owner)
                .setAttribute('target.bucket',
                    bucketData.target.bucket)
                .setAttribute('target.key', deleteMarker.Key)
                .setAttribute('target.accountId',
                    bucketData.target.accountId)
                .setAttribute('target.version',
                    deleteMarker.VersionId);
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

    _getRules(bucketData, bucketLCRules, object) {
        const filteredRules = this._lifecycleUtils.filterRules(bucketLCRules, object, object.Tags);
        return this._lifecycleUtils.getApplicableRules(filteredRules, object);
    }
}

module.exports = LifecycleTaskV2;
