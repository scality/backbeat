'use strict'; // eslint-disable-line

const async = require('async');
const { errors } = require('arsenal');

const { attachReqUids } = require('../../../lib/clients/utils');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');

// Default max AWS limit is 1000 for both list objects and list object versions
const MAX_KEYS = process.env.CI === 'true' ? 3 : 1000;
// concurrency mainly used in async calls
const CONCURRENCY_DEFAULT = 10;

function isLifecycleUser(canonicalID) {
    const canonicalIDArray = canonicalID.split('/');
    const serviceName = canonicalIDArray[canonicalIDArray.length - 1];
    return serviceName === 'lifecycle';
}

class LifecycleTask extends BackbeatTask {
    /**
     * Processes Kafka Bucket entries and determines if specific Lifecycle
     * actions apply to an object, version of an object, or MPU.
     *
     * @constructor
     * @param {LifecycleBucketProcessor} lp - lifecycle processor instance
     */
    constructor(lp) {
        const lpState = lp.getStateVars();
        super();
        Object.assign(this, lpState);
    }

    /**
     * Send entry back to bucket task topic
     * @param {Object} entry - The Kafka entry to send to the topic
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _sendBucketEntry(entry, cb) {
        const entries = [{ message: JSON.stringify(entry) }];
        this.producer.sendToTopic(this.bucketTasksTopic, entries, cb);
    }

    /**
     * Send entry to the object task topic
     * @param {ActionQueueEntry} entry - The action entry to send to the topic
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _sendObjectAction(entry, cb) {
        const entries = [{ message: entry.toKafkaMessage() }];
        this.producer.sendToTopic(this.objectTasksTopic, entries, cb);
    }

    /**
     * Send entry to the data mover topic
     * @param {ActionQueueEntry} entry - The action entry to send to the topic
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _sendDataMoverAction(entry, cb) {
        const { bucket, key } = entry.getAttribute('target');
        const entries = [{ key: `${bucket}/${key}`,
                           message: entry.toKafkaMessage() }];
        this.producer.sendToTopic(this.dataMoverTopic, entries, cb);
    }

    /**
     * Handles non-versioned objects
     * @param {object} bucketData - bucket data
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _getObjectList(bucketData, bucketLCRules, log, done) {
        const params = {
            Bucket: bucketData.target.bucket,
            MaxKeys: MAX_KEYS,
        };
        if (bucketData.details.marker) {
            params.Marker = bucketData.details.marker;
        }

        const req = this.s3target.listObjects(params);
        attachReqUids(req, log);
        async.waterfall([
            next => req.send((err, data) => {
                if (err) {
                    log.error('error listing bucket objects', {
                        method: 'LifecycleTask._getObjectList',
                        error: err,
                        bucket: params.Bucket,
                    });
                    return next(err);
                }
                return next(null, data);
            }),
            (data, next) => {
                if (data.IsTruncated) {
                    // re-queue to Kafka topic bucketTasksTopic
                    // with bucket name and `data.marker`
                    let marker = data.NextMarker;
                    if (!marker && data.Contents.length > 0) {
                        marker = data.Contents[data.Contents.length - 1].Key;
                    }

                    const entry = Object.assign({}, bucketData, {
                        details: { marker },
                    });
                    this._sendBucketEntry(entry, err => {
                        if (!err) {
                            log.debug(
                                'sent kafka entry for bucket consumption', {
                                    method: 'LifecycleTask._getObjectList',
                                });
                        }
                    });
                }

                this._compareRulesToList(bucketData, bucketLCRules,
                    data.Contents, log, 'Disabled', next);
            },
        ], done);
    }

    /**
     * Handles versioned objects (both enabled and suspended)
     * @param {object} bucketData - bucket data
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {string} versioningStatus - 'Enabled' or 'Suspended'
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _getObjectVersions(bucketData, bucketLCRules, versioningStatus, log, done) {
        const paramDetails = {};

        if (bucketData.details.versionIdMarker &&
        bucketData.details.keyMarker) {
            paramDetails.KeyMarker = bucketData.details.keyMarker;
            paramDetails.VersionIdMarker = bucketData.details.versionIdMarker;
        }

        this._listVersions(bucketData, paramDetails, log, (err, data) => {
            if (err) {
                // error already logged at source
                return done(err);
            }
            // all versions including delete markers
            const allVersions = this._mergeSortedVersionsAndDeleteMarkers(
                data.Versions, data.DeleteMarkers);
            // for all versions and delete markers, add stale date property
            const allVersionsWithStaleDate = this._addStaleDateToVersions(
                bucketData.details, allVersions);

            // sending bucket entry for checking next listing
            if (data.IsTruncated && allVersions.length > 0) {
                // Uses last version whether Version or DeleteMarker
                const last = allVersions[allVersions.length - 1];
                const entry = Object.assign({}, bucketData, {
                    details: {
                        keyMarker: data.NextKeyMarker,
                        versionIdMarker: data.NextVersionIdMarker,
                        prevDate: last.LastModified,
                    },
                });
                this._sendBucketEntry(entry, err => {
                    if (!err) {
                        log.debug('sent kafka entry for bucket ' +
                        'consumption', {
                            method: 'LifecycleTask._getObjectVersions',
                        });
                    }
                });
            }

            // if no versions to process, skip further processing for this
            // batch
            if (allVersionsWithStaleDate.length === 0) {
                return done(null);
            }

            // for each version, get their relative rules, compare with
            // bucket rules, match with `staleDate` to
            // NoncurrentVersionExpiration Days and send expiration if
            // rules all apply
            return this._compareRulesToList(bucketData, bucketLCRules,
                allVersionsWithStaleDate, log, versioningStatus, done);
        });
    }

    /**
     * Handles incomplete multipart uploads
     * @param {object} bucketData - bucket data
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {object} params - params for AWS S3 `listObjectVersions`
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _getMPUs(bucketData, bucketLCRules, params, log, done) {
        const req = this.s3target.listMultipartUploads(params);
        attachReqUids(req, log);
        async.waterfall([
            next => req.send((err, data) => {
                if (err) {
                    log.error('error checking buckets MPUs', {
                        method: 'LifecycleTask._getMPUs',
                        error: err,
                        bucket: params.Bucket,
                    });
                    return next(err);
                }
                return next(null, data);
            }),
            (data, next) => {
                if (data.IsTruncated) {
                    // re-queue to kafka with `NextUploadIdMarker` &
                    // `NextKeyMarker`
                    const entry = Object.assign({}, bucketData, {
                        details: {
                            keyMarker: data.NextKeyMarker,
                            uploadIdMarker: data.NextUploadIdMarker,
                        },
                    });
                    return this._sendBucketEntry(entry, err => {
                        if (!err) {
                            log.debug(
                                'sent kafka entry for bucket consumption', {
                                    method: 'LifecycleTask._getMPUs',
                                });
                        }
                        return next(null, data);
                    });
                }
                return process.nextTick(() => next(null, data));
            },
        ], (err, data) => {
            if (err) {
                return done(err);
            }

            this._compareMPUUploads(bucketData, bucketLCRules,
                data.Uploads, log);
            return done();
        });
    }

    /**
     * Helper method to merge and sort Versions and DeleteMarkers lists
     * @param {array} versions - versions list
     * @param {array} deleteMarkers - delete markers list
     * @return {array} merge sorted array
     */
    _mergeSortedVersionsAndDeleteMarkers(versions, deleteMarkers) {
        const sortedList = [];
        // Version index counter
        let vIdx = 0;
        // Delete Marker index counter
        let dmIdx = 0;

        while (vIdx < versions.length || dmIdx < deleteMarkers.length) {
            if (versions[vIdx] === undefined) {
                // versions list is empty, just need to merge remaining DM's
                sortedList.push(deleteMarkers[dmIdx++]);
            } else if (deleteMarkers[dmIdx] === undefined) {
                // DM's list is empty, just need to merge remaining versions
                sortedList.push(versions[vIdx++]);
            } else if (versions[vIdx].Key !== deleteMarkers[dmIdx].Key) {
                // 1. by Key name, alphabetical order sorted by ascii values
                const isVKeyNewer = (versions[vIdx].Key <
                    deleteMarkers[dmIdx].Key);
                if (isVKeyNewer) {
                    sortedList.push(versions[vIdx++]);
                } else {
                    sortedList.push(deleteMarkers[dmIdx++]);
                }
            } else {
                //    NOTE: VersionId may be null
                const nullVersion = (versions[vIdx].VersionId === 'null'
                    || deleteMarkers[dmIdx].VersionId === 'null');
                const isVersionVidNewer = (versions[vIdx].VersionId <
                    deleteMarkers[dmIdx].VersionId);

                // if there is a null version, handle accordingly
                if (nullVersion) {
                    const isVersionLastModifiedNewer =
                        (new Date(versions[vIdx].LastModified) >
                         new Date(deleteMarkers[dmIdx].LastModified));
                    const isDMLastModifiedNewer =
                        (new Date(deleteMarkers[dmIdx].LastModified) >
                         new Date(versions[vIdx].LastModified));
                     // 2. by LastModified, find newer
                    if (isVersionLastModifiedNewer) {
                        sortedList.push(versions[vIdx++]);
                    } else if (isDMLastModifiedNewer) {
                        sortedList.push(deleteMarkers[dmIdx++]);
                    } else {
                        // 3. choose one randomly since all conditions match
                        // TODO: to be fixed
                        sortedList.push(versions[vIdx++]);
                    }
                } else {
                    // 4. by VersionId, lower number means newer
                    if (isVersionVidNewer) {
                        sortedList.push(versions[vIdx++]);
                    } else {
                        sortedList.push(deleteMarkers[dmIdx++]);
                    }
                }
            }
        }

        return sortedList;
    }

    /**
     * Helper method to add a staleDate property to each Version and
     * DeleteMarker
     * @param {object} bucketDetails - details property from Kafka Bucket entry
     * @param {string} [bucketDetails.keyMarker] - previous listing key name
     * @param {string} [bucketDetails.prevDate] - previous listing LastModified
     * @param {array} list - list of sorted versions and delete markers
     * @return {array} an updated array of Versions and DeleteMarkers with
     *   applied staleDate
     */
    _addStaleDateToVersions(bucketDetails, list) {
        const appliedList = [];

        for (let i = 0; i < list.length; i++) {
            const dupe = Object.assign({}, list[i]);

            if (dupe.IsLatest) {
                // IsLatest version should not have a staleDate
                dupe.staleDate = undefined;
            } else if (i === 0) {
                // first item in list. bucket details may apply
                dupe.staleDate = (bucketDetails.keyMarker === dupe.Key) ?
                    bucketDetails.prevDate : undefined;
            } else {
                dupe.staleDate = list[i - 1].LastModified;
            }

            appliedList.push(dupe);
        }

        return appliedList;
    }

    /**
     * Wrapper for AWS S3 listObjectVersions
     * @param {object} bucketData - bucket data
     * @param {object} paramDetails - any extra param details (i.e. key marker,
     *   version id marker, prefix)
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} cb - cb(error, dataList)
     * @return {undefined}
     */
    _listVersions(bucketData, paramDetails, log, cb) {
        const params = {
            Bucket: bucketData.target.bucket,
            MaxKeys: MAX_KEYS,
        };
        if (paramDetails.VersionIdMarker && paramDetails.KeyMarker) {
            params.KeyMarker = paramDetails.KeyMarker;
            params.VersionIdMarker = paramDetails.VersionIdMarker;
        }
        if (paramDetails.Prefix) {
            params.Prefix = paramDetails.Prefix;
        }

        const req = this.s3target.listObjectVersions(params);
        attachReqUids(req, log);
        req.send((err, data) => {
            if (err) {
                log.error('error listing versioned bucket objects', {
                    method: 'LifecycleTask._listVersions',
                    error: err,
                    bucket: params.Bucket,
                    prefix: params.Prefix,
                });
                return cb(err);
            }
            return cb(null, data);
        });
    }

    /**
     * Filter out all rules based on `Status` and `Filter` (Prefix and Tags)
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {object} item - represents a single object, version, or upload
     * @param {object} objTags - all tags for given `item`
     * @return {array} list of all filtered rules that apply to `item`
     */
    _filterRules(bucketLCRules, item, objTags) {
        /*
            Bucket Tags must be included in the list of object tags.
            So if a bucket tag with "key1/value1" exists, and an object with
            "key1/value1, key2/value2" exists, this bucket lifecycle rules
            apply on this object.

            Vice versa, bucket rule is "key1/value1, key2/value2" and object
            rule is "key1/value1", this buckets rule does not apply to this
            object.
        */
        function deepCompare(rTags, oTags) {
            // check to make sure object tags length matches or is greater
            if (rTags.length > oTags.length) {
                return false;
            }
            // all key/value tags of bucket rules must be within object tags
            for (let i = 0; i < rTags.length; i++) {
                const oTag = oTags.find(pair => pair.Key === rTags[i].Key);
                if (!oTag || rTags[i].Value !== oTag.Value) {
                    return false;
                }
            }
            return true;
        }

        return bucketLCRules.filter(rule => {
            if (rule.Status === 'Disabled') {
                return false;
            }
            // check all locations where prefix could possibly be
            const prefix = rule.Prefix ||
                  (rule.Filter && (rule.Filter.And ?
                                   rule.Filter.And.Prefix :
                                   rule.Filter.Prefix));
            if (prefix && !item.Key.startsWith(prefix)) {
                return false;
            }
            if (!rule.Filter) {
                return true;
            }
            const tags = rule.Filter.And ?
                  rule.Filter.And.Tags :
                  (rule.Filter.Tag && [rule.Filter.Tag]);
            if (tags && !deepCompare(tags, objTags.TagSet || [])) {
                return false;
            }
            return true;
        });
    }

    /**
     * For all filtered rules, get rules that apply the earliest
     * @param {array} rules - list of filtered rules that apply to a specific
     *   object, version, or upload
     * @param {object} metadata - metadata about the object to transition
     * @return {object} all applicable rules with earliest dates of action
     *  i.e. { Expiration: { Date: <DateObject>, Days: 10 },
     *         NoncurrentVersionExpiration: { NoncurrentDays: 5 } }
     */
    _getApplicableRules(rules, metadata) {
        // NOTE: Ask Team
        // Assumes if for example a rule defines expiration and transition
        // and if backbeat disables expiration and enables transition,
        // we still consider this rules transition to apply
        // these are rules enabled in config.json
        const enabledRules = Object.keys(this.enabledRules).filter(rule =>
            this.enabledRules[rule].enabled);

        // Declare the current date before the reducing function so that all
        // rule comparisons use the same date.
        const currentDate = new Date();
        /* eslint-disable no-param-reassign */
        const applicableRules = rules.reduce((store, rule) => {
            // filter and find earliest dates
            if (rule.Expiration && enabledRules.includes('expiration')) {
                if (!store.Expiration) {
                    store.Expiration = {};
                }
                if (rule.Expiration.Days) {
                    if (!store.Expiration.Days || rule.Expiration.Days <
                    store.Expiration.Days) {
                        store.Expiration.Days = rule.Expiration.Days;
                    }
                }
                if (rule.Expiration.Date) {
                    if (!store.Expiration.Date || rule.Expiration.Date <
                    store.Expiration.Date) {
                        store.Expiration.Date = rule.Expiration.Date;
                    }
                }
                const eodm = rule.Expiration.ExpiredObjectDeleteMarker;
                if (eodm !== undefined) {
                    // preference for later rules in list of rules
                    store.Expiration.ExpiredObjectDeleteMarker = eodm;
                }
            }
            if (rule.NoncurrentVersionExpiration &&
            enabledRules.includes('noncurrentVersionExpiration')) {
                // Names are long, so obscuring a bit
                const ncve = 'NoncurrentVersionExpiration';
                const ncd = 'NoncurrentDays';

                if (!store[ncve]) {
                    store[ncve] = {};
                }
                if (!store[ncve][ncd] || rule[ncve][ncd] < store[ncve][ncd]) {
                    store[ncve][ncd] = rule[ncve][ncd];
                }
            }
            if (rule.AbortIncompleteMultipartUpload &&
            enabledRules.includes('abortIncompleteMultipartUpload')) {
                // Names are long, so obscuring a bit
                const aimu = 'AbortIncompleteMultipartUpload';
                const dai = 'DaysAfterInitiation';

                if (!store[aimu]) {
                    store[aimu] = {};
                }
                if (!store[aimu][dai] || rule[aimu][dai] < store[aimu][dai]) {
                    store[aimu][dai] = rule[aimu][dai];
                }
            }
            const hasTransitions =
                Array.isArray(rule.Transitions) && rule.Transitions.length > 0;
            if (hasTransitions && enabledRules.includes('transitions')) {
                store.Transition = this._getApplicableTransition({
                    transitions: rule.Transitions,
                    lastModified: metadata.LastModified,
                    store,
                    currentDate,
                });
            }
            // TODO: Add support for NoncurrentVersionTransitions.
            return store;
        }, {});
        // Do not transition to a location where the object is already stored.
        if (applicableRules.Transition &&
            applicableRules.Transition.StorageClass === metadata.StorageClass) {
            applicableRules.Transition = undefined;
        }
        return applicableRules;
        /* eslint-enable no-param-reassign */
    }

    /**
     * Get the Unix timestamp of the given date.
     * @param {string} date - The date string to convert to a Unix timestamp
     * @return {number} - The Unix timestamp
     */
    _getTimestamp(date) {
        return new Date(date).getTime();
    }

    /**
     * Find the Unix time at which the transition should occur.
     * @param {object} transition - A transition from the lifecycle transitions
     * @param {string} lastModified - The object's last modified date
     * @return {number|undefined} - The normalized transition timestamp
     */
    _getTransitionTimestamp(transition, lastModified) {
        if (transition.Date !== undefined) {
            return this._getTimestamp(transition.Date);
        }
        if (transition.Days !== undefined) {
            const lastModifiedTime = this._getTimestamp(lastModified);
            const oneDay = 24 * 60 * 60 * 1000; // Milliseconds in a day.
            return lastModifiedTime + (transition.Days * oneDay);
        }
        return undefined;
    }

    /**
     * Find the most relevant trantition rule for the given transitions array
     * and any previously stored transition from another rule.
     * @param {object} params - The function parameters
     * @param {array} params.transitions - Array of lifecycle rule transitions
     * @param {string} params.lastModified - The object's last modified
     * date
     * @return {object} The most applicable transition rule
     */
    _getApplicableTransition(params) {
        const { transitions, store, lastModified, currentDate } = params;
        const transition = transitions.reduce((result, transition) => {
            const isApplicable = // Is the transition time in the past?
                this._getTimestamp(currentDate) >=
                this._getTransitionTimestamp(transition, lastModified);
            if (!isApplicable) {
                return result;
            }
            return this._compareTransitions({
                transition1: transition,
                transition2: result,
                lastModified,
            });
        }, undefined);
        return this._compareTransitions({
            transition1: transition,
            transition2: store.Transition,
            lastModified,
        });
    }

    /**
     * Compare two transition rules and return the one that is most recent.
     * @param {object} params - The function parameters
     * @param {object} params.transition1 - A transition from the current rule
     * @param {object} params.transition2 - A transition from the previous rule
     * @param {string} params.lastModified - The object's last modified
     * date
     * @return {object} The most applicable transition rule
     */
    _compareTransitions(params) {
        const { transition1, transition2, lastModified } = params;
        if (transition1 === undefined) {
            return transition2;
        }
        if (transition2 === undefined) {
            return transition1;
        }
        return this._getTransitionTimestamp(transition1, lastModified) >
            this._getTransitionTimestamp(transition2, lastModified) ?
            transition1 : transition2;
    }

    /**
     * Handles comparing rules for objects
     * @param {object} bucketData - bucket data
     * @param {object} bucketData.target - target bucket info
     * @param {string} bucketData.target.bucket - bucket name
     * @param {string} bucketData.target.owner - owner id
     * @param {string} [bucketData.prefix] - prefix
     * @param {string} [bucketData.details.keyMarker] - next key
     *   marker for versioned buckets
     * @param {string} [bucketData.details.versionIdMarker] - next
     *   version id marker for versioned buckets
     * @param {string} [bucketData.details.marker] - next
     *   continuation token marker for non-versioned buckets
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {object} object - object or object version
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _getRules(bucketData, bucketLCRules, object, log, done) {
        if (this._isDeleteMarker(object)) {
            // DeleteMarkers don't have any tags, so avoid calling
            // `getObjectTagging` which will throw an error
            const filterRules = this._filterRules(bucketLCRules, object, []);
            return done(null, this._getApplicableRules(filterRules, object));
        }

        const tagParams = { Bucket: bucketData.target.bucket, Key: object.Key };
        if (object.VersionId) {
            tagParams.VersionId = object.VersionId;
        }

        const req = this.s3target.getObjectTagging(tagParams);
        attachReqUids(req, log);
        return req.send((err, tags) => {
            if (err) {
                log.error('failed to get tags', {
                    method: 'LifecycleTask._getRules',
                    error: err,
                    bucket: bucketData.target.bucket,
                    objectKey: object.Key,
                    objectVersion: object.VersionId,
                });
                return done(err);
            }
            // tags.TagSet === [{ Key: '', Value: '' }, ...]
            const filterRules = this._filterRules(bucketLCRules, object, tags);

            // reduce filteredRules to only get earliest dates
            return done(null, this._getApplicableRules(filterRules, object));
        });
    }


    /**
     * Get rules and compare with each object or version
     * @param {object} bucketData - bucket data
     * @param {array} lcRules - array of bucket lifecycle rules
     * @param {array} contents - list of objects or object versions
     * @param {Logger.newRequestLogger} log - logger object
     * @param {string} versioningStatus - 'Enabled', 'Suspended', or 'Disabled'
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _compareRulesToList(bucketData, lcRules, contents, log, versioningStatus,
    done) {
        if (!contents.length) {
            return done();
        }
        return async.eachLimit(contents, CONCURRENCY_DEFAULT, (obj, cb) => {
            async.waterfall([
                next => this._getRules(bucketData, lcRules, obj, log, next),
                (applicableRules, next) => {
                    let rules = applicableRules;
                    // Hijack for testing
                    // Idea is to set any "Days" rule to `Days - 1`
                    const testIsOn = process.env.CI === 'true';
                    if (testIsOn) {
                        rules = this._adjustRulesForTesting(rules);
                    }

                    if (versioningStatus === 'Enabled' ||
                    versioningStatus === 'Suspended') {
                        return this._compareVersion(bucketData, obj, contents,
                            rules, versioningStatus, log, next);
                    }
                    return this._compareObject(bucketData, obj, rules, log,
                        next);
                },
            ], cb);
        }, done);
    }

    /**
     * Helper method to get total Days passed since given date
     * @param {Date} date - date object
     * @return {number} Days passed
     */
    _findDaysSince(date) {
        const now = Date.now();
        const diff = now - date;
        return Math.floor(diff / (1000 * 60 * 60 * 24));
    }

    /**
     * Helper method to determine if a version is a Delete Marker
     * @param {Object} version - single version object
     * @return {boolean} true/false
     */
    _isDeleteMarker(version) {
        // if no ETag, Size, and StorageClass, then it is a Delete Marker
        return (
            !Object.prototype.hasOwnProperty.call(version, 'ETag') &&
            !Object.prototype.hasOwnProperty.call(version, 'Size') &&
            !Object.prototype.hasOwnProperty.call(version, 'StorageClass')
        );
    }

    /**
     * Helper method for Expiration.Date and Expiration.Days rules
     * Check if Expiration rules apply on the object or version
     * @param {object} bucketData - bucket data
     * @param {object} obj - single object or version
     * @param {string} obj.LastModified - last modified date of object
     * @param {object} rules - most applicable rules from `_getApplicableRules`
     * @param {Logger.newRequestLogger} log - logger object
     * @return {boolean} used to know if a rule has been applied already.
     *   If this rule has been applied to the object or version, other rules
     *   (i.e. transition) should not apply.
     */
    _checkAndApplyExpirationRule(bucketData, obj, rules, log) {
        const daysSinceInitiated = this._findDaysSince(
            new Date(obj.LastModified));

        if (rules.Expiration.Date &&
            rules.Expiration.Date < Date.now()) {
            // expiration date passed for this object
            const entry = ActionQueueEntry.create('deleteObject')
                  .addContext({
                      origin: 'lifecycle',
                      ruleType: 'expiration',
                      reqId: log.getSerializedUids(),
                  })
                  .setAttribute('target.owner', bucketData.target.owner)
                  .setAttribute('target.bucket', bucketData.target.bucket)
                  .setAttribute('target.key', obj.Key)
                  .setAttribute('details.lastModified', obj.LastModified);
            this._sendObjectAction(entry, err => {
                if (!err) {
                    log.debug('sent object entry for consumption',
                    Object.assign({
                        method: 'LifecycleTask._checkAndApplyExpirationRule',
                    }, entry.getLogInfo()));
                }
            });
            return true;
        }
        if (rules.Expiration.Days !== undefined &&
        daysSinceInitiated >= rules.Expiration.Days) {
            const entry = ActionQueueEntry.create('deleteObject')
                  .addContext({
                      origin: 'lifecycle',
                      ruleType: 'expiration',
                      reqId: log.getSerializedUids(),
                  })
                  .setAttribute('target.owner', bucketData.target.owner)
                  .setAttribute('target.bucket', bucketData.target.bucket)
                  .setAttribute('target.key', obj.Key)
                  .setAttribute('details.lastModified', obj.LastModified);
            this._sendObjectAction(entry, err => {
                if (!err) {
                    log.debug('sent object entry for consumption',
                    Object.assign({
                        method: 'LifecycleTask._checkAndApplyExpirationRule',
                    }, entry.getLogInfo()));
                }
            });
            return true;
        }
        return false;
    }

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
     * @param {string} params.site - The site name to transition the object to
     * @param {Werelogs.Logger} log - Logger object
     * @return {undefined}
     */
    _applyTransitionRule(params, log) {
        const entry = ActionQueueEntry.create('copyLocation')
              .setResultsTopic(this.objectTasksTopic)
              .addContext({
                  origin: 'lifecycle',
                  ruleType: 'transition',
                  reqId: log.getSerializedUids(),
              })
              .setAttribute('target.bucket', params.bucket)
              .setAttribute('target.key', params.objectKey)
              .setAttribute('target.version', params.encodedVersionId)
              .setAttribute('target.eTag', params.eTag)
              .setAttribute('toLocation', params.site);
        this._sendDataMoverAction(entry, err => {
            if (err) {
                log.error('could not send transition entry for consumption',
                          Object.assign({
                              method: 'LifecycleTask._applyTransitionRule',
                              error: err,
                          }, entry.getLogInfo()));
            }
            log.debug('sent transition entry for consumption', Object.assign({
                method: 'LifecycleTask._applyTransitionRule',
            }, entry.getLogInfo()));
        });
    }

    /**
     * Helper method for Expiration.ExpiredObjectDeleteMarker rule
     * Check if ExpiredObjectDeleteMarker rule applies to the `IsLatest` delete
     * marker
     * @param {object} bucketData - bucket data
     * @param {object} deleteMarker - single non-current version
     * @param {string} deleteMarker.Key - key name
     * @param {string} deleteMarker.VersionId - version id
     * @param {array} listOfVersions - versions and delete markers from listing
     * @param {object} rules - most applicable rules from `_getApplicableRules`
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error)
     * @return {undefined}
     */
    _checkAndApplyEODMRule(bucketData, deleteMarker, listOfVersions, rules, log,
    done) {
        // If the delete marker is the last item in the list of versions, there
        // may be other matching keys in the next listing. Will need to list
        // with prefix for this case.
        const lastVersion = listOfVersions[listOfVersions.length - 1];

        async.waterfall([
            next => {
                if (lastVersion.Key === deleteMarker.Key &&
                lastVersion.VersionId === deleteMarker.VersionId) {
                    const param = { Prefix: deleteMarker.Key };
                    return this._listVersions(bucketData, param, log,
                    (err, data) => {
                        if (err) {
                            // error already logged at source
                            return done(err);
                        }
                        const allVersions = [...data.Versions,
                            ...data.DeleteMarkers];
                        return next(null, allVersions);
                    });
                }
                return next(null, listOfVersions);
            },
            (versions, next) => {
                const matchingNoncurrentKeys = versions.filter(v => (
                    v.Key === deleteMarker.Key && !v.IsLatest));
                const daysSinceInitiated = this._findDaysSince(
                    new Date(deleteMarker.LastModified));

                const eodm = rules.Expiration &&
                    rules.Expiration.ExpiredObjectDeleteMarker;

                const applicableExpRule = rules.Expiration && (
                    (rules.Expiration.Days !== undefined &&
                     daysSinceInitiated >= rules.Expiration.Days) ||
                    (rules.Expiration.Date !== undefined &&
                     rules.Expiration.Date < Date.now()) ||
                    eodm !== false
                );
                const validLifecycleUserCase = (
                    isLifecycleUser(deleteMarker.Owner.ID) &&
                    eodm !== false
                );

                // if there are no other versions with the same Key as this DM,
                // if a valid Expiration rule exists or if the DM was created
                // by a lifecycle service account and eodm rule is not
                // explicitly set to false, apply and permanently delete this DM
                if (matchingNoncurrentKeys.length === 0 && (applicableExpRule ||
                validLifecycleUserCase)) {
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
                          .setAttribute('target.version',
                                        deleteMarker.VersionId);
                    this._sendObjectAction(entry, err => {
                        if (!err) {
                            log.debug('sent object entry for consumption',
                            Object.assign({
                                method: 'LifecycleTask._checkAndApplyEODMRule',
                            }, entry.getLogInfo()));
                        }
                    });
                }
                next();
            },
        ], done);
    }

    /**
     * Helper method for NoncurrentVersionExpiration.NoncurrentDays rule
     * Check if Noncurrent Expiration rule applies on the version
     * @param {object} bucketData - bucket data
     * @param {object} version - single non-current version
     * @param {string} version.LastModified - last modified date of version
     * @param {object} rules - most applicable rules from `_getApplicableRules`
     * @param {Logger.newRequestLogger} log - logger object
     * @return {undefined}
     */
    _checkAndApplyNCVExpirationRule(bucketData, version, rules, log) {
        const staleDate = version.staleDate;
        const daysSinceInitiated = this._findDaysSince(new Date(staleDate));
        const ncve = 'NoncurrentVersionExpiration';
        const ncd = 'NoncurrentDays';
        const doesNCVExpirationRuleApply = (rules[ncve] &&
            rules[ncve][ncd] !== undefined &&
            daysSinceInitiated >= rules[ncve][ncd]);
        if (doesNCVExpirationRuleApply) {
            const entry = ActionQueueEntry.create('deleteObject')
                  .addContext({
                      origin: 'lifecycle',
                      ruleType: 'expiration',
                      reqId: log.getSerializedUids(),
                  })
                  .setAttribute('target.owner', bucketData.target.owner)
                  .setAttribute('target.bucket', bucketData.target.bucket)
                  .setAttribute('target.key', version.Key)
                  .setAttribute('target.version', version.VersionId);
            this._sendObjectAction(entry, err => {
                if (!err) {
                    log.debug('sent object entry for consumption',
                    Object.assign({
                        method: 'LifecycleTask._checkAndApplyNCVExpirationRule',
                    }, entry.getLogInfo()));
                }
            });
        }
    }

    /**
     * Compare a non-versioned object to most applicable rules
     * @param {object} bucketData - bucket data
     * @param {object} obj - single object from `listObjects`
     * @param {string} obj.LastModified - last modified date of object
     * @param {object} rules - most applicable rules from `_getApplicableRules`
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _compareObject(bucketData, obj, rules, log, done) {
        const params = {
            Bucket: bucketData.target.bucket,
            Key: obj.Key,
        };
        // Used by `IsLatest` version
        if (obj.VersionId) {
            params.VersionId = obj.VersionId;
        }
        const req = this.s3target.headObject(params);
        attachReqUids(req, log);
        return req.send((err, data) => {
            if (err) {
                log.error('failed to get object', {
                    method: 'LifecycleTask._compareObject',
                    error: err,
                    bucket: bucketData.target.bucket,
                    objectKey: obj.Key,
                });
                return done(err);
            }

            const object = Object.assign({}, obj,
                { LastModified: data.LastModified });

            // There is an order of importance in cases of conflicts
            // Expiration and NoncurrentVersionExpiration should be priority
            // AbortIncompleteMultipartUpload should run regardless since
            // it's in its own category
            if (rules.Expiration) {
                this._checkAndApplyExpirationRule(bucketData, object, rules,
                    log);
                return done();
            }
            if (rules.Transition) {
                this._applyTransitionRule({
                    bucket: bucketData.target.bucket,
                    objectKey: obj.Key,
                    eTag: obj.ETag,
                    site: rules.Transition.StorageClass,
                }, log);
                return done();
            }

            return done();
        });
    }

    /**
     * Only to be used when testing (when process.env.CI is true).
     * The idea is to adjust any "Days" or "NoncurrentDays" rules so that rules
     * set with 1 day should expire, but any days set with 2+ days will not.
     * Since Days/NoncurrentDays cannot be set to 0, this is a way to set the
     * rule and test methods are working as intended.
     * @param {object} rules - applicable rules
     * @return {object} adjusted rules object
     */
    _adjustRulesForTesting(rules) {
        /* eslint-disable no-param-reassign */
        if (rules.Expiration &&
        rules.Expiration.Days) {
            rules.Expiration.Days--;
        }
        const ncve = 'NoncurrentVersionExpiration';
        const ncd = 'NoncurrentDays';
        if (rules[ncve] &&
        rules[ncve][ncd]) {
            rules[ncve][ncd]--;
        }
        const aimu = 'AbortIncompleteMultipartUpload';
        const dai = 'DaysAfterInitiation';
        if (rules[aimu] &&
        rules[aimu][dai]) {
            rules[aimu][dai]--;
        }
        /* eslint-enable no-param-reassign */
        return rules;
    }

    /**
     * Compare a version to most applicable rules
     * @param {object} bucketData - bucket data
     * @param {object} version - single version from `_getObjectVersions`
     * @param {array} listOfVersions - versions and delete markers from listing
     * @param {object} rules - most applicable rules from `_getApplicableRules`
     * @param {string} versioningStatus - 'Enabled' or 'Suspended'
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _compareVersion(bucketData, version, listOfVersions, rules,
    versioningStatus, log, done) {
        // if version is latest, only expiration action applies
        if (version.IsLatest) {
            return this._compareIsLatestVersion(bucketData, version,
                listOfVersions, rules, versioningStatus, log, done);
        }

        if (!version.staleDate) {
            // NOTE: this should never happen. Logging here for debug purposes
            log.error('missing staleDate on the version', {
                method: 'LifecycleTask._compareVersion',
                bucket: bucketData.target.bucket,
                versionId: version.VersionId,
            });
            const errMsg = 'an implementation error occurred: when comparing ' +
                'lifecycle rules on a version, stale date was missing';
            return done(errors.InternalError.customizeDescription(errMsg));
        }

        // TODO: Add support for NoncurrentVersionTransitions.
        this._checkAndApplyNCVExpirationRule(bucketData, version, rules, log);

        return done();
    }

    /**
     * Compare the `IsLatest` version to most applicable rules. Also handles
     * the `ExpiredObjectDeleteMarker` lifecycle rule.
     * @param {object} bucketData - bucket data
     * @param {object} version - single version from `_getObjectVersions`
     * @param {array} listOfVersions - versions and delete markers from listing
     * @param {object} rules - most applicable rules from `_getApplicableRules`
     * @param {string} versioningStatus - 'Enabled' or 'Suspended'
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error)
     * @return {undefined}
     */
    _compareIsLatestVersion(bucketData, version, listOfVersions, rules,
    versioningStatus, log, done) {
        const isDeleteMarker = this._isDeleteMarker(version);

        if (isDeleteMarker) {
            // check EODM
            return this._checkAndApplyEODMRule(bucketData, version,
                listOfVersions, rules, log, done);
        }
        // if Expiration rule exists, apply it here to a Version
        if (rules.Expiration) {
            this._checkAndApplyExpirationRule(bucketData, version, rules,
                log);
            return done();
        }
        if (rules.Transition) {
            this._applyTransitionRule({
                bucket: bucketData.target.bucket,
                objectKey: version.Key,
                eTag: version.ETag,
                site: rules.Transition.StorageClass,
                encodedVersionId: undefined,
            }, log);
            return done();
        }

        log.debug('no action taken on IsLatest version', {
            bucket: bucketData.target.bucket,
            key: version.Key,
            versioningStatus,
        });
        return done();
    }

    /**
     * For all incomplete MPU uploads, compare with rules (based only on prefix
     * because no tags exist for incomplete MPU), and apply
     * AbortIncompleteMultipartUpload rule (if any) on each upload
     * @param {object} bucketData - bucket data
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {array} uploads - array of upload objects from
     *   `listMultipartUploads`
     * @param {Logger.newRequestLogger} log - logger object
     * @return {undefined}
     */
    _compareMPUUploads(bucketData, bucketLCRules, uploads, log) {
        uploads.forEach(upload => {
            // Tags do not apply to UploadParts
            const noTags = { TagSet: [] };
            const filteredRules = this._filterRules(bucketLCRules, upload,
                noTags);
            let aRules = this._getApplicableRules(filteredRules, {});

            // Hijack for testing
            // Idea is to set any "Days" rule to `Days - 1`
            const testIsOn = process.env.CI === 'true';
            if (testIsOn) {
                aRules = this._adjustRulesForTesting(aRules);
            }

            const daysSinceInitiated = this._findDaysSince(
                new Date(upload.Initiated));
            const abortRule = aRules.AbortIncompleteMultipartUpload;

            // NOTE: DaysAfterInitiation can be 0 in tests
            const doesAbortRuleApply = (abortRule &&
                abortRule.DaysAfterInitiation !== undefined &&
                daysSinceInitiated >= abortRule.DaysAfterInitiation);
            if (doesAbortRuleApply) {
                log.debug('send mpu upload for aborting', {
                    bucket: bucketData.target.bucket,
                    method: 'LifecycleTask._compareMPUUploads',
                    uploadId: upload.UploadId,
                });
                const entry = ActionQueueEntry.create('deleteMPU')
                      .addContext({
                          origin: 'lifecycle',
                          ruleType: 'expiration',
                          reqId: log.getSerializedUids(),
                      })
                      .setAttribute('target.owner', bucketData.target.owner)
                      .setAttribute('target.bucket', bucketData.target.bucket)
                      .setAttribute('target.key', upload.Key)
                      .setAttribute('details.UploadId', upload.UploadId);
                this._sendObjectAction(entry, err => {
                    if (!err) {
                        log.debug('sent object entry for consumption',
                            Object.assign({
                                method: 'LifecycleTask._compareMPUUploads',
                            }, entry.getLogInfo()));
                    }
                });
            }
        });
    }

    /**
     * Process a single bucket entry with lifecycle configurations enabled
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {object} bucketData - bucket data from zookeeper bucketTasksTopic
     * @param {object} bucketData.target - target bucket info
     * @param {string} bucketData.target.bucket - bucket name
     * @param {string} bucketData.target.owner - owner id
     * @param {string} [bucketData.details.prefix] - prefix
     * @param {string} [bucketData.details.keyMarker] - next key
     *   marker for versioned buckets
     * @param {string} [bucketData.details.versionIdMarker] - next version id
     *   marker for versioned buckets
     * @param {string} [bucketData.details.marker] - next continuation token
     *   marker for non-versioned buckets
     * @param {string} [bucketData.details.uploadIdMarker] - ext upload id
     *   marker for MPU
     * @param {string} [bucketData.details.prevDate] - used specifically for
     *   handling versioned buckets
     * @param {string} [bucketData.details.objectName] - used specifically for
     *   handling versioned buckets
     * @param {AWS.S3} s3target - s3 instance
     * @param {function} done - callback(error)
     * @return {undefined}
     */
    processBucketEntry(bucketLCRules, bucketData, s3target, done) {
        const log = this.log.newRequestLogger();
        this.s3target = s3target;

        if (!this.s3target) {
            return process.nextTick(done);
        }
        if (typeof bucketData !== 'object' ||
            typeof bucketData.target !== 'object' ||
            typeof bucketData.details !== 'object') {
            log.error('wrong format for bucket entry',
                      { entry: bucketData });
            return process.nextTick(done);
        }

        log.debug('processing bucket entry', {
            bucket: bucketData.target.bucket,
            owner: bucketData.target.owner,
        });

        // Initially, processing a Bucket entry should check mpu AND
        // (versioned OR non-versioned) objects
        return async.series([
            cb => {
                // if any of these markers exists on the Bucket entry, the entry
                // is handling a specific request that is not an MPU request
                if (bucketData.details.versionIdMarker ||
                bucketData.details.marker) {
                    return cb();
                }
                const mpuParams = {
                    Bucket: bucketData.target.bucket,
                    MaxUploads: MAX_KEYS,
                    KeyMarker: bucketData.details.uploadIdMarker &&
                        bucketData.details.keyMarker,
                    UploadIdMarker: bucketData.details.keyMarker &&
                        bucketData.details.uploadIdMarker,
                };
                return this._getMPUs(bucketData, bucketLCRules,
                    mpuParams, log, cb);
            },
            cb => {
                // if this marker exists on the Bucket entry, the entry is
                // handling an MPU request
                if (bucketData.details.uploadIdMarker) {
                    return cb();
                }

                return async.waterfall([
                    next => {
                        const req = this.s3target.getBucketVersioning({
                            Bucket: bucketData.target.bucket,
                        });
                        attachReqUids(req, log);
                        req.send((err, data) => {
                            if (err) {
                                log.error('error checking bucket versioning', {
                                    method: 'LifecycleTask.processBucketEntry',
                                    error: err,
                                });
                                return next(err);
                            }
                            return next(null, data.Status);
                        });
                    },
                    (versioningStatus, next) => {
                        if (versioningStatus === 'Enabled' ||
                        versioningStatus === 'Suspended') {
                            return this._getObjectVersions(bucketData,
                                bucketLCRules, versioningStatus, log, next);
                        }

                        return this._getObjectList(bucketData, bucketLCRules,
                            log, next);
                    },
                ], cb);
            },
        ], err => {
            this.log.info('finished processing task for bucket lifecycle', {
                method: 'LifecycleTask.processBucketEntry',
                bucket: bucketData.target.bucket,
                owner: bucketData.target.owner,
            });
            return done(err);
        });
    }
}

module.exports = LifecycleTask;
