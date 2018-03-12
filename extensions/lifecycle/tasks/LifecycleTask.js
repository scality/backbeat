'use strict'; // eslint-disable-line

const async = require('async');

const attachReqUids = require('../../replication/utils/attachReqUids');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');

const RETRYTIMEOUTS = 300;

// Default max AWS limit is 1000 for both list objects and list object versions
const MAX_KEYS = 1000;
// concurrency mainly used in async calls
const CONCURRENCY_DEFAULT = 10;

class LifecycleTask extends BackbeatTask {
    /**
     * Process a list of versioned or non-versioned objects
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
     * @param {object} params - params for AWS S3 `listObjects`
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _getObjectList(bucketData, params, log, done) {
        const req = this.s3target.listObjects(params);
        attachReqUids(req, log);
        async.waterfall([
            next => req.send((err, data) => {
                if (err) {
                    log.error('error listing bucket objects', {
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
                    return this.sendBucketEntry(entry, err => {
                        if (!err) {
                            log.debug(
                                'sent kafka entry for bucket consumption', {
                                    method: 'LifecycleTask._getObjectList',
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
            return done(null, { Contents: data.Contents });
        });
    }

    /**
     * Handles versioned objects (both enabled and suspended)
     * @param {object} bucketData - bucket data
     * @param {object} params - params for AWS S3 `listObjectVersions`
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _getObjectVersions(bucketData, params, log, done) {
        const req = this.s3target.listObjectVersions(params);
        attachReqUids(req, log);
        async.waterfall([
            next => req.send((err, data) => {
                if (err) {
                    log.error('error listing versioned bucket objects', {
                        error: err,
                        bucket: params.Bucket,
                    });
                    return next(err);
                }
                return next(null, data);
            }),
            (data, next) => {
                if (data.IsTruncated) {
                    // NOTE: this might only be needed if a LC rule defines
                    //   a rule for processing NoncurrentVersion actions
                    const entry = Object.assign({}, bucketData, {
                        details: {
                            keyMarker: data.NextKeyMarker,
                            versionIdMarker: data.NextVersionIdMarker,
                        },
                    });
                    return this.sendBucketEntry(entry, err => {
                        if (!err) {
                            log.debug(
                                'sent kafka entry for bucket consumption', {
                                    method: 'LifecycleTask._getObjectVersions',
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
            return done(null, {
                Versions: data.Versions,
                DeleteMarkers: data.DeleteMarkers,
            });
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
                        method: 'LifecycleTask.processBucketEntry',
                        error: err,
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
            const noTags = { TagSet: [] };
            const filteredRules = this._filterRules(bucketLCRules, upload,
                noTags);
            const aRules = this._getApplicableRules(filteredRules);

            const timeDiff = (Date.now() - new Date(upload.Initiated));
            const daysSinceInitiated = Math.floor(
                timeDiff / (1000 * 60 * 60 * 24));
            const abortRule = aRules.AbortIncompleteMultipartUpload;

            if (abortRule && abortRule.DaysAfterInitiation &&
            daysSinceInitiated >= abortRule.DaysAfterInitiation) {
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
     * Filter out all rules based on `Status` and `Filter` (Prefix and Tags)
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {object} item - represents a single object, version, or upload
     * @param {object} objTags - all tags for given `item`
     * @return {array} list of all filtered rules that apply to `item`
     */
    _filterRules(bucketLCRules, item, objTags) {
        function deepCompare(rTags, oTags) {
            // check key lengths match
            if (rTags.length !== oTags.length) {
                return false;
            }
            // all key/value tags must match between the 2 sets
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
            if (objTags.TagSet.length > 1) {
                // can only have one tag at this point in time
                return false;
            }
            if (rule.Filter && rule.Filter.Tag && objTags.TagSet[0]) {
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
            // TODO: consider `Date`
            const objDate = new Date(data.LastModified);
            const now = Date.now();
            // time diff in ms
            const diff = now - objDate;
            const daysSinceInitiated = Math.floor(diff / (1000 * 60 * 60 * 24));

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
                if (rules.Expiration.Date && rules.Expiration.Date < now) {
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
                if (!alreadySent && rules.Expiration.Days &&
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

    _compareContents(bucketData, lcRules, contents, log, done) {
        if (!contents) {
            return done();
        }
        return async.eachLimit(contents, CONCURRENCY_DEFAULT, (obj, cb) => {
            async.waterfall([
                next => this._getRules(bucketData, lcRules, obj, log, next),
                (applicableRules, next) => this._compareObject(
                    bucketData, obj, applicableRules, log, next),
            ], cb);
        }, done);
    }

    _compareVersions(bucketData, lcRules, versions, log, done) {
        if (!versions) {
            return done();
        }
        return async.eachLimit(versions, CONCURRENCY_DEFAULT, (version, cb) => {
            async.waterfall([
                next => this._getRules(bucketData, lcRules, version, log, next),
                (applicableRules, next) => {
                    // if version is latest, only expiration action applies
                    if (version.IsLatest) {
                        return this._compareObject(
                            bucketData, version, applicableRules, log, next);
                    }
                    const req = this.s3target.headObject({
                        Bucket: bucketData.target.bucket,
                        Key: version.Key,
                        VersionId: version.VersionId,
                    });
                    attachReqUids(req, log);
                    return req.send((err, data) => {
                        if (err) {
                            log.error('failed to get object', {
                                method: 'LifecycleTask._compareVersions',
                                error: err,
                                bucket: bucketData.target.bucket,
                                objectKey: version.Key,
                                versionId: version.VersionId,
                            });
                            return next(err);
                        }
                        const objDate = new Date(data.LastModified);
                        const now = Date.now();
                        // time diff in ms
                        const diff = now - objDate;
                        const daysSinceInitiated =
                            Math.floor(diff / (1000 * 60 * 60 * 24));
                        const ncve = 'NoncurrentVersionExpiration';
                        const ncd = 'NoncurrentDays';
                        if (applicableRules[ncve] &&
                        applicableRules[ncve][ncd] &&
                        daysSinceInitiated >= applicableRules[ncve][ncd]) {
                            const entry = {
                                action: 'deleteObject',
                                target: {
                                    owner: bucketData.target.owner,
                                    bucket: bucketData.target.bucket,
                                    key: version.Key,
                                    version: data.VersionId,
                                },
                            };
                            this.sendObjectEntry(entry, err => {
                                if (!err) {
                                    log.debug('sent object entry for ' +
                                    'consumption', {
                                        method: 'LifecycleTask.' +
                                            '_compareVersions',
                                        entry,
                                    });
                                }
                            });
                        }
                        return next();
                    });
                },
            ], cb);
        }, done);
    }

    _compareDeleteMarkers(bucketData, lcRules, deleteMarkers, versions,
        log, done) {
        // TODO: Issue with rule `Expiration.ExpiredObjectDeleteMarker`
        // will be completed later
        return done();
    }

    _compareWithLCRules(bucketData, bucketLCRules, data, log, cb) {
        // These could be executed in parallel, but to have better
        // control on how much parallelism we introduce overall we do
        // these steps in series for now.
        async.series([
            done => this._compareContents(bucketData, bucketLCRules,
                data.Contents, log, done),
            done => this._compareVersions(bucketData, bucketLCRules,
                data.Versions, log, done),
            done => this._compareDeleteMarkers(bucketData, bucketLCRules,
                data.DeleteMarkers, data.Versions, log, done),
        ], cb);
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
     * @param {string} [bucketData.details.versionIdMarker] - next
     *   version id marker for versioned buckets
     * @param {string} [bucketData.details.marker] - next
     *   continuation token marker for non-versioned buckets
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

        // NOTE:
        //  With the addition of mpu's, there was no easy way to handle
        //  how to process entries w/ markers. Will need to rethink this and
        //  refactor

        // get all objects whether versioned, non-versioned, or mpu
        return async.series([
            cb => {
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
                if (bucketData.details.uploadIdMarker) {
                    return cb();
                }

                const params = {
                    Bucket: bucketData.target.bucket,
                    MaxKeys: MAX_KEYS,
                };

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
                            // handle versioned stuff
                            if (bucketData.details.versionIdMarker &&
                            bucketData.details.keyMarker) {
                                params.KeyMarker =
                                    bucketData.details.keyMarker;
                                params.VersionIdMarker =
                                    bucketData.details.versionIdMarker;
                            }
                            return this._getObjectVersions(bucketData, params,
                                log, next);
                        }
                        // handle non-versioned stuff
                        if (bucketData.details.marker) {
                            params.Marker =
                                bucketData.details.marker;
                        }
                        return this._getObjectList(bucketData, params, log,
                            next);
                    },
                    // Got a list of objects or versions to process now..
                    (data, next) => this._compareWithLCRules(bucketData,
                        bucketLCRules, data, log, next),
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
