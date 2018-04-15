'use strict'; // eslint-disable-line

const async = require('async');
const { errors } = require('arsenal');

const attachReqUids = require('../../replication/utils/attachReqUids');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');

const RETRYTIMEOUTS = 300;

// Default max AWS limit is 1000 for both list objects and list object versions
const MAX_KEYS = process.env.TEST_SWITCH ? 3 : 1000;
// concurrency mainly used in async calls
const CONCURRENCY_DEFAULT = 10;

class LifecycleTask extends BackbeatTask {
    /**
     * Processes Kafka Bucket entries and determines if specific Lifecycle
     * actions apply to an object, version of an object, or MPU.
     *
     * @constructor
     * @param {LifecycleProducer} lp - lifecycle producer instance
     */
    constructor(lp) {
        const lpState = lp.getStateVars();
        super({ retryTimeoutS: RETRYTIMEOUTS });
        Object.assign(this, lpState);
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
                    this.sendBucketEntry(entry, err => {
                        if (!err) {
                            log.debug(
                                'sent kafka entry for bucket consumption', {
                                    method: 'LifecycleTask._getObjectList',
                                });
                        }
                    });
                }

                this._applyRulesToList(bucketData, bucketLCRules, data.Contents,
                    log, false, next);
            },
        ], done);
    }

    /**
     * Handles versioned objects (both enabled and suspended)
     * @param {object} bucketData - bucket data
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _getObjectVersions(bucketData, bucketLCRules, log, done) {
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
            // for all versions and delete markers, apply stale date property
            const allVersionsWithStaleDate = this._applyVersionStaleDate(
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
                this.sendBucketEntry(entry, err => {
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
            return this._applyRulesToList(bucketData, bucketLCRules,
                allVersionsWithStaleDate, log, true, done);
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
                    return this.sendBucketEntry(entry, err => {
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
        let vIdx = 0;
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
                // 2. by VersionId, lower number means newer
                const isVersionVidNewer = (versions[vIdx].VersionId <
                    deleteMarkers[dmIdx].VersionId);
                if (isVersionVidNewer) {
                    sortedList.push(versions[vIdx++]);
                } else {
                    sortedList.push(deleteMarkers[dmIdx++]);
                }
            }
        }

        return sortedList;
    }

    /**
     * Helper method to apply a staleDate property to each Version and
     * DeleteMarker
     * @param {object} bucketDetails - details property from Kafka Bucket entry
     * @param {string} [bucketDetails.keyMarker] - previous listing key name
     * @param {string} [bucketDetails.prevDate] - previous listing LastModified
     * @param {array} list - list of sorted versions and delete markers
     * @return {array} an updated array of Versions and DeleteMarkers with
     *   applied staleDate
     */
    _applyVersionStaleDate(bucketDetails, list) {
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
            if (rule.Filter && rule.Filter.And) {
                // multiple tags or prefix w/ tag(s)
                const prefix = rule.Filter.And.Prefix;
                if (prefix && !item.Key.startsWith(prefix)) {
                    return false;
                }
                return deepCompare(rule.Filter.And.Tags, objTags.TagSet);
            }
            // only one prefix or one tag to compare in this rule
            const prefix = rule.Prefix ||
                (rule.Filter && rule.Filter.Prefix);
            if (prefix && !item.Key.startsWith(prefix)) {
                return false;
            }
            if (rule.Filter && rule.Filter.Tag && objTags.TagSet) {
                return deepCompare([rule.Filter.Tag], objTags.TagSet);
            }
            return true;
        });
    }

    /**
     * For all filtered rules, get rules that apply the earliest
     * @param {array} rules - list of filtered rules that apply to a specific
     *   object, version, or upload
     * @return {object} all applicable rules with earliest dates of action
     *  i.e. { Expiration: { Date: <DateObject>, Days: 10 },
     *         NoncurrentVersionExpiration: { NoncurrentDays: 5 } }
     */
    _getApplicableRules(rules) {
        // NOTE: Ask Team
        // Assumes if for example a rule defines expiration and transition
        // and if backbeat disables expiration and enables transition,
        // we still consider this rules transition to apply

        // these are rules enabled in config.json
        const enabledRules = Object.keys(this.enabledRules).filter(rule =>
            this.enabledRules[rule].enabled);

        /* eslint-disable no-param-reassign */
        return rules.reduce((store, rule) => {
            // filter and find earliest dates
            // NOTE: only care about expiration for this feature.
            //  Add other lifecycle rules here for future features.

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
                if (rule.Expiration.ExpiredObjectDeleteMarker) {
                    store.Expiration.ExpiredObjectDeleteMarker = true;
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
            return store;
        }, {});
        /* eslint-enable no-param-reassign */
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
     * @param {array} object - object or object version
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _getRules(bucketData, bucketLCRules, object, log, done) {
        // if no ETag, Size, and StorageClass, then it is a Delete Marker
        const isDeleteMarker = (
            !Object.prototype.hasOwnProperty.call(object, 'ETag') &&
            !Object.prototype.hasOwnProperty.call(object, 'Size') &&
            !Object.prototype.hasOwnProperty.call(object, 'StorageClass'));
        if (isDeleteMarker) {
            // DeleteMarkers don't have any tags, so avoid calling
            // `getObjectTagging` which will throw an error
            const filterRules = this._filterRules(bucketLCRules, object, []);
            return done(null, this._getApplicableRules(filterRules));
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
            return done(null, this._getApplicableRules(filterRules));
        });
    }


    /**
     * Get rules and compare with each object or version
     * @param {object} bucketData - bucket data
     * @param {array} lcRules - array of bucket lifecycle rules
     * @param {array} contents - list of objects or object versions
     * @param {Logger.newRequestLogger} log - logger object
     * @param {boolean} versioned - tells if contents are versioned list or not
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _applyRulesToList(bucketData, lcRules, contents, log, versioned, done) {
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
                    const testIsOn = process.env.TEST_SWITCH === '1';
                    if (testIsOn) {
                        rules = this._adjustRulesForTesting(rules);
                    }

                    if (versioned) {
                        return this._compareVersion(bucketData, obj, rules, log,
                            next);
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
            const daysSinceInitiated = this._findDaysSince(
                new Date(data.LastModified));

            /* Example of `rules`
                {
                    Expiration: {
                        Days: 5,
                        Date: <DateObject>,
                        ExpiredObjectDeleteMarker: true,
                    },
                    NoncurrentVersionExpiration: {
                        NoncurrentDays: 2,
                    },
                    AbortIncompleteMultipartUpload: {
                        DaysAfterInitiation: 4,
                    }
                }
            */
            // There is an order of importance in cases of conflicts
            // Expiration and NoncurrentVersionExpiration should be priority
            // AbortIncompleteMultipartUpload should run regardless since
            // it's in its own category
            // Transitions and NoncurrentVersionTransitions (which we don't
            // need to worry about this release) should only happen if
            // no expiration occurred
            let alreadySent = false;

            if (rules.Expiration) {
                // TODO: Handle ExpiredObjectDeleteMarker
                if (rules.Expiration.Date &&
                rules.Expiration.Date < Date.now()) {
                    // expiration date passed for this object
                    alreadySent = true;
                    const entry = {
                        action: 'deleteObject',
                        target: {
                            owner: bucketData.target.owner,
                            bucket: bucketData.target.bucket,
                            key: obj.Key,
                        },
                        details: {
                            lastModified: data.LastModified,
                        },
                    };
                    this.sendObjectEntry(entry, err => {
                        if (!err) {
                            log.debug('sent object entry for consumption', {
                                method: 'LifecycleTask._compareObject',
                                entry,
                            });
                        }
                    });
                }
                if (!alreadySent && rules.Expiration.Days !== undefined &&
                daysSinceInitiated >= rules.Expiration.Days) {
                    alreadySent = true;
                    const entry = {
                        action: 'deleteObject',
                        target: {
                            owner: bucketData.target.owner,
                            bucket: bucketData.target.bucket,
                            key: obj.Key,
                        },
                        details: {
                            lastModified: data.LastModified,
                        },
                    };
                    this.sendObjectEntry(entry, err => {
                        if (!err) {
                            log.debug('sent object entry for consumption', {
                                method: 'LifecycleTask._compareObject',
                                entry,
                            });
                        }
                    });
                }
            }
            // if (rules.Transitions && !alreadySent) {}

            return done();
        });
    }

    /**
     * Only to be used when testing (when process.env.TEST_SWITCH).
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
     * @param {object} rules - most applicable rules from `_getApplicableRules`
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _compareVersion(bucketData, version, rules, log, done) {
        // if version is latest, only expiration action applies
        if (version.IsLatest) {
            // TODO: handle unique cases for comparing expiration rule
            return done();
        }

        const staleDate = version.staleDate;
        if (!staleDate) {
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

        const daysSinceInitiated = this._findDaysSince(new Date(staleDate));
        const ncve = 'NoncurrentVersionExpiration';
        const ncd = 'NoncurrentDays';
        const doesNCVExpirationRuleApply = (rules[ncve] &&
            rules[ncve][ncd] !== undefined &&
            daysSinceInitiated >= rules[ncve][ncd]);
        if (doesNCVExpirationRuleApply) {
            const entry = {
                action: 'deleteObject',
                target: {
                    owner: bucketData.target.owner,
                    bucket: bucketData.target.bucket,
                    key: version.Key,
                    version: version.VersionId,
                },
            };
            this.sendObjectEntry(entry, err => {
                if (!err) {
                    log.debug('sent object entry for ' +
                    'consumption', {
                        method: 'LifecycleTask._compareVersion',
                        entry,
                    });
                }
            });
        }
        return done();
    }

    _compareDeleteMarkers(bucketData, lcRules, deleteMarkers, versions,
        log, done) {
        // TODO: Issue with rule `Expiration.ExpiredObjectDeleteMarker`
        // will be completed later
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
            const aRules = this._getApplicableRules(filteredRules);

            const daysSinceInitiated = this._findDaysSince(
                new Date(upload.Initiated));
            const abortRule = aRules.AbortIncompleteMultipartUpload;
            const doesAbortRuleApply = (abortRule &&
                abortRule.DaysAfterInitiation !== undefined &&
                daysSinceInitiated >= abortRule.DaysAfterInitiation);
            if (doesAbortRuleApply) {
                log.debug('send mpu upload for aborting', {
                    bucket: bucketData.target.bucket,
                    method: 'LifecycleTask._compareMPUUploads',
                    uploadId: upload.UploadId,
                });
                const entry = {
                    action: 'deleteMPU',
                    target: {
                        owner: bucketData.target.owner,
                        bucket: bucketData.target.bucket,
                        key: upload.Key,
                    },
                    details: {
                        UploadId: upload.UploadId,
                    },
                };
                this.sendObjectEntry(entry, err => {
                    if (!err) {
                        log.debug('sent object entry for consumption', {
                            method: 'LifecycleTask._compareMPUUploads',
                            entry,
                        });
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
                    UploadIdMarker: bucketData.details.uploadIdMarker,
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
                                bucketLCRules, log, next);
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
