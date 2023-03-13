'use strict'; // eslint-disable-line

const async = require('async');
const { errors } = require('arsenal');

const LifecycleTask = require('./LifecycleTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { reduceRulesForVersionedBucket, rulesToParams } = require('../util/rulesReducer');
const { lifecycleListing: { NON_CURRENT_TYPE, CURRENT_TYPE, ORPHAN_TYPE } } = require('../../../lib/constants');



// Default max AWS limit is 1000 for both list objects and list object versions
const MAX_KEYS = process.env.CI === 'true' ? 3 : 1000;
// concurrency mainly used in async calls
const CONCURRENCY_DEFAULT = 10;
// moves lifecycle transition deadlines 1 day earlier, mostly for testing
const transitionOneDayEarlier = process.env.TRANSITION_ONE_DAY_EARLIER === 'true';
// moves lifecycle expiration deadlines 1 day earlier, mostly for testing
const expireOneDayEarlier = process.env.EXPIRE_ONE_DAY_EARLIER === 'true';

function isLifecycleUser(canonicalID) {
    const canonicalIDArray = canonicalID.split('/');
    const serviceName = canonicalIDArray[canonicalIDArray.length - 1];
    return serviceName === 'lifecycle';
}

class LifecycleTaskV2 extends LifecycleTask {

    _sendRemainingBucketEntries(nbRetries, remainings, bucketData, log) {
        if (nbRetries === 0 && remainings.length) {
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
        const currentDate = Date.now();

        const { params, listingDetails, remainings } = rulesToParams('Disabled', currentDate, bucketLCRules, bucketData);

        this._sendRemainingBucketEntries(nbRetries, remainings, bucketData, log);

        // nb of listings is limited per bucket with reducer.
        this.backbeatMetadataProxy.listLifecycleMasters(params, log, (err, data) => {
            if (err) {
                // TODO: what should we do if the listing fail?
                return done(err);
            }

            console.log('1 MASTER listingDetails.listType!!!', listingDetails.listType);
            console.log('2 MASTER data!!!', data);

            // re-queue to Kafka topic bucketTasksTopic
            // with bucket name and `data.marker` only once.
            if (data.IsTruncated && data.NextMarker && nbRetries === 0) {
                const entry = Object.assign({}, bucketData, {
                    details: { ...listingDetails, marker: data.NextMarker },
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
            console.log('data.Contents!!!', data.Contents);
            // TODO: compare objects to Rules
            return this._compareRulesToList(bucketData, bucketLCRules,
                data.Contents, log, done);
        });
    }

    _getObjectVersions(bucketData, bucketLCRules, versioningStatus, nbRetries, log, done) {
        const currentDate = Date.now();

        const { params, listingDetails, remainings } =
            rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        console.log('GET_LISTING!!!', { params, listingDetails, remainings });

        this._sendRemainingBucketEntries(nbRetries, remainings, bucketData, log);

        // nb of listings is limited per bucket with reducer.
        this.backbeatMetadataProxy.listLifecycle(listingDetails.listType, params, log, 
        (err, contents, isTruncated, markerInfo) => {
            if (err) {
                // TODO: what should we do if the listing fail?
                return done(err);
            }
            console.log('1 VERSION listingDetails.listType!!!', listingDetails.listType);

            // re-queue to Kafka topic bucketTasksTopic
            // with bucket name and `data.marker` only once.
            if (isTruncated && nbRetries === 0) {
                const entry = Object.assign({}, bucketData, {
                    details: {
                        ...listingDetails,
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
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */

    _compareCurrent(bucketData, obj, rules, log, cb) {
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
        // if version is latest, only expiration action applies
        if (obj.ListType === ORPHAN_TYPE) {
            this._checkAndApplyEODMRule(bucketData, obj, rules, log);
            return process.nextTick(cb);
        }

        if (obj.ListType === CURRENT_TYPE) {
            return this._compareCurrent(bucketData, obj, rules, log, cb);
        }

        if (obj.ListType === NON_CURRENT_TYPE) {
            return this._compareNonCurrent(bucketData, obj, rules, log, cb);
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

    // _getTransitionActionEntry(params, log, cb) {
    //     let attempt;
    //     const rawAttempt = params.transitionAttempt;
    //     if (rawAttempt) {
    //         attempt = Number.parseInt(rawAttempt, 10);
    //     }

    //     const entry = ReplicationAPI.createCopyLocationAction({
    //         bucketName: params.bucket,
    //         owner: params.owner,
    //         objectKey: params.objectKey,
    //         versionId: params.versionId,
    //         eTag: params.eTag,
    //         lastModified: params.lastModified,
    //         toLocation: params.site,
    //         originLabel: 'lifecycle',
    //         fromLocation: params.siteFrom,
    //         contentLength: params.size,
    //         resultsTopic: this.objectTasksTopic,
    //         accountId: params.accountId,
    //         attempt,
    //     });
    //     entry.addContext({
    //         origin: 'lifecycle',
    //         ruleType: 'transition',
    //         reqId: log.getSerializedUids(),
    //     });

    //     if (this._canUnconditionallyGarbageCollect(params)) {
    //         return cb(null, entry);
    //     }
    //     return this._getObjectMD(params, log, (err, objectMD) => {
    //         if (err) {
    //             return cb(err);
    //         }
    //         const locations = objectMD.getLocation();
    //         return this._headLocation(params, locations, log,
    //             (err, lastModified) => {
    //                 if (err) {
    //                     return cb(err);
    //                 }
    //                 entry.setAttribute('source', {
    //                     bucket: params.bucket,
    //                     objectKey: params.objectKey,
    //                     storageClass: objectMD.getDataStoreName(),
    //                     lastModified,
    //                 });
    //                 return cb(null, entry);
    //         });
    //     });
    // }

    // _canUnconditionallyGarbageCollect(params) {
    //     const sourceEndpoint = config.getBootstrapList()
    //         .find(endpoint => endpoint.site === params.siteFrom);
    //     // Is it a local data source?
    //     if (!sourceEndpoint) {
    //         return true;
    //     }
    //     // Is the public cloud data source versioned?
    //     if (params.isDataStoreVersionedId) {
    //         return true;
    //     }
    //     return false;
    // }

    /**
     * Gets the transition entry and sends it to the data mover topic,
     * then gathers the result in the object tasks topic for execution
     * by the lifecycle object processor to update object metadata.
     *
     * @param {object} params - The function parameters
     * @param {string} params.bucket - The source bucket name
     * @param {string} params.objectKey - The object key name
     * @param {string} params.encodedVersionId - The object encoded version ID
     * @param {string} params.eTag - The object data ETag
     * @param {string} params.lastModified - The last modified date of object
     * @param {string} params.site - The site name to transition the object to
     * @param {Werelogs.Logger} log - Logger object
     * @return {undefined}
     */
    // _applyTransitionRule(params, log) {
    //     async.waterfall([
    //         next => {
    //             // TODO: add the following and clean it:
    //             const { siteFrom, transitionInProgress, restoreCompletedAt } = params;
    //             console.log('_applyTransitionRule => PARAM!!!!', params);
    //             const isObjectCold = siteFrom && locationsConfig[siteFrom]
    //                 && locationsConfig[siteFrom].isCold;
    //             // We do not transition cold objects
    //             if (isObjectCold) {
    //                 return next(errorTransitionColdObject);
    //             }
    //             // If transition is in progress, do not re-publish entry
    //             // to data-mover or cold-archive topic.
    //             if (transitionInProgress) {
    //                 return next(errorTransitionInProgress);
    //             }

    //             // If object is temporarily restored, don't try
    //             // to transition it again.
    //             if (restoreCompletedAt) {
    //                 return next(errorObjectTemporarilyRestored);
    //             }

    //             return this._getTransitionActionEntry(params, log, (err, entry) =>
    //                 next(err, entry));
    //         },
    //         (entry, next) =>
    //             ReplicationAPI.sendDataMoverAction(this.producer, entry, log, err =>
    //                 next(err, entry)),
    //         (entry, next) => {
    //             // Update object metadata with "x-amz-scal-transition-in-progress"
    //             // to avoid transitioning object a second time from a new batch.
    //             // Only implemented for transitions to cold location.
    //             const toLocation = entry.getAttribute('toLocation');
    //             const locationConfig = locationsConfig[toLocation];
    //             if (locationConfig && locationConfig.isCold) {
    //                     return this._getObjectMD(params, log, (err, objectMD) => {
    //                         if (err) {
    //                             return next(err);
    //                         }
    //                         LifecycleMetrics.onS3Request(log, 'getMetadata', 'bucket', err);
    //                         objectMD.setTransitionInProgress(true);
    //                         const putParams = {
    //                             bucket: params.bucket,
    //                             objectKey: params.objectKey,
    //                             versionId: params.versionId,
    //                             mdBlob: objectMD.getSerialized(),
    //                         };
    //                         return this._putObjectMD(putParams, log, err => {
    //                             LifecycleMetrics.onS3Request(log, 'putMetadata', 'bucket', err);
    //                             return next(err);
    //                         });
    //                     });
    //             }

    //             return process.nextTick(next);
    //         }
    //     ], err => {
    //         if (err) {
    //             // FIXME: this can get verbose with expected errors
    //             // such as temporarily restored objects. A flag
    //             // needs to be added to expected errors.
    //             log.error('could not apply transition rule', {
    //                 method: 'LifecycleTask._applyTransitionRule',
    //                 error: err.description || err.message,
    //                 owner: params.owner,
    //                 bucket: params.bucket,
    //                 key: params.objectKey,
    //                 site: params.site,
    //             });
    //         } else {
    //             log.debug('transition rule applied', {
    //                 method: 'LifecycleTask._applyTransitionRule',
    //                 owner: params.owner,
    //                 bucket: params.bucket,
    //                 key: params.objectKey,
    //                 site: params.site,
    //             });
    //         }
    //     });
    // }

    _getRules(bucketData, bucketLCRules, object) {
        const filteredRules = this._lifecycleUtils.filterRules(bucketLCRules, object, object.Tags);
        return this._lifecycleUtils.getApplicableRules(filteredRules, object);
    }
}

module.exports = LifecycleTaskV2;
