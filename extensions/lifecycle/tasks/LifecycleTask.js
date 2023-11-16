'use strict'; // eslint-disable-line

const async = require('async');
const { errors, versioning } = require('arsenal');
const { ObjectMD } = require('arsenal').models;
const { supportedLifecycleRules } = require('arsenal').constants;
const {
    LifecycleDateTime,
    LifecycleUtils,
} = require('arsenal').s3middleware.lifecycleHelpers;
const { CompareResult, MinHeap } = require('arsenal').algorithms.Heap;

const config = require('../../../lib/Config');
const { attachReqUids } = require('../../../lib/clients/utils');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const ReplicationAPI = require('../../replication/ReplicationAPI');
const { LifecycleMetrics } = require('../LifecycleMetrics');
const locationsConfig = require('../../../conf/locationConfig.json') || {};
const { decode } = versioning.VersionID;

const errorTransitionInProgress = errors.InternalError.
    customizeDescription('transition is currently in progress');
const errorTransitionColdObject = errors.InternalError.
    customizeDescription('transitioning a cold object is forbidden');
const errorObjectTemporarilyRestored = errors.InternalError.
    customizeDescription('object temporarily restored');
const errorReplicationInProgress = errors.InternalError.
    customizeDescription('replication of the object is currently in progress');
const errorLocationPaused = errors.InternalError.
    customizeDescription('lifecycle events to location have been paused');

// Default max AWS limit is 1000 for both list objects and list object versions
const MAX_KEYS = process.env.CI === 'true' ? 3 : 1000;
// concurrency mainly used in async calls
const CONCURRENCY_DEFAULT = 10;

// Max number of retries (with exp. backoff) that can be be performed for a single
// entry. We don't use the default, to avoid retrying for a long time (5 minutes)
// which could make Kafka timeout. 4 retries (e.g. 5 attempts) should be around 8
// seconds.
const MAX_RETRIES = 4;

// Maximum number of retries when processing entries in the range.
// We will retry a few times, but limit the total number of retries to ensure the
// range is processed timely. Not retrying is not too bad, as the next run will.
// We are processing 10 entries at a time, so a range should take around 1 second.
// Since entries get processed in parallel, they will get distributed accross the
// parallel tasks, so the total delay of retries should about 1m30s.
const MAX_RETRIES_TOTAL = CONCURRENCY_DEFAULT * MAX_RETRIES * 10;

/**
 * compare 2 version by their stale dates returning:
 * - LT (-1) if v1 is less than v2
 * - EQ (0) if v1 equals v2
 * - GT (1) if v1 is greater than v2
 * @param {object} v1 - object version
 * @param {object} v2 - object version
 * @returns {number} -
 */
function noncurrentVersionCompare(v1, v2) {
    const v1Date = new Date(v1.staleDate);
    const v2Date = new Date(v2.staleDate);

    if (v1Date < v2Date) {
        return CompareResult.LT;
    }

    if (v1Date > v2Date) {
        return CompareResult.GT;
    }

    return CompareResult.EQ;
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

        const { transitionOneDayEarlier, expireOneDayEarlier, timeProgressionFactor } = this.lcOptions;
        this._lifecycleDateTime = new LifecycleDateTime({
            transitionOneDayEarlier,
            expireOneDayEarlier,
            timeProgressionFactor,
        });

        this._lifecycleUtils = new LifecycleUtils(
            supportedLifecycleRules,
            this._lifecycleDateTime
        );
        this._supportedRules = supportedLifecycleRules;
        this._totalRetries = 0;
    }

    setSupportedRules(supportedRules) {
        this._lifecycleUtils = new LifecycleUtils(
            supportedRules,
            this._lifecycleDateTime
        );
        this._supportedRules = supportedRules;
    }

    /**
     * Send entry back to bucket task topic
     * @param {Object} entry - The Kafka entry to send to the topic
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _sendBucketEntry(entry, cb) {
        const entries = [{ message: JSON.stringify(entry) }];
        this.producer.sendToTopic(this.bucketTasksTopic, entries, err => {
            LifecycleMetrics.onKafkaPublish(null, 'BucketTopic', 'bucket', err, 1);
            return cb(err);
        });
    }

    /**
     * This function forces syncing the latest data mover topic
     * offsets to the 'lifecycle' metrics snapshot. It is called when
     * a bucket listing completes.
     *
     * By doing this, we ensure that when the bucket tasks queue is
     * fully processed (no lag), the snapshot of data mover offsets
     * will be beyond all transition tasks to be executed by the data
     * mover, hence the conductor can check whether anything remains
     * to be transitioned by the data mover (and skip the next task if
     * so).
     *
     *
     * @param {Werelogs.Logger} log - Logger object
     * @return {undefined}
     */
    _snapshotDataMoverTopicOffsets(log) {
        // TODO: if multiple topics are used in replication API, i.e, one per
        // location (implemented during cold-storage epic), handle them separately
        this.kafkaBacklogMetrics.snapshotTopicOffsets(
            this.producer.getKafkaProducer(),
            ReplicationAPI.getDataMoverTopic(), 'lifecycle', err => {
                if (err) {
                    log.error('error during snapshot of topic offsets', {
                        topic: ReplicationAPI.getDataMoverTopic(),
                        error: err.message,
                    });
                }
            });
    }

    /**
     * Send entry to the object task topic
     * @param {ActionQueueEntry} entry - The action entry to send to the topic
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _sendObjectAction(entry, cb) {
        LifecycleMetrics.onLifecycleTriggered(this.log, 'bucket',
            entry.getActionType() === 'deleteMPU' ? 'expiration:mpu' : 'expiration',
            entry.getAttribute('details.dataStoreName'),
            Date.now() - entry.getAttribute('transitionTime'));

        const entries = [{ message: entry.toKafkaMessage() }];
        this.producer.sendToTopic(this.objectTasksTopic, entries,  err => {
            LifecycleMetrics.onKafkaPublish(null, 'ObjectTopic', 'bucket', err, 1);
            return cb(err);
        });
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
                LifecycleMetrics.onS3Request(log, 'listObjects', 'bucket', err);

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
                if (data.IsTruncated && nbRetries === 0) {
                    // re-queue to Kafka topic bucketTasksTopic
                    // with bucket name and `data.marker` only once.
                    let marker = data.NextMarker;
                    if (!marker && data.Contents.length > 0) {
                        marker = data.Contents[data.Contents.length - 1].Key;
                    }

                    const entry = Object.assign({}, bucketData, {
                        contextInfo: { reqId: log.getSerializedUids() },
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
     * Adds object to the noncurrent version helper Heap object.
     * If the heap cap is reached:
     * - compare the smallest object and the current object
     *   - if the current object is smaller:
     *      - remove top object and add the current object into the heap
     *      - return top object to be expired
     *  - if the top of the heap is smaller:
     *      - return the current object to be expired
     * If the heap cap has not been reached:
     * - add the current object into the heap and return null
     * @param {string} bucketName - bucket name
     * @param {object} rule - rule object
     * @param {object} version - object version
     * @return {object | null} - null or the version to be expired
     */
    _ncvHeapAdd(bucketName, rule, version) {
        const ncve = 'NoncurrentVersionExpiration';
        const nncv = 'NewerNoncurrentVersions';

        if (!rule[ncve] || !rule[ncve][nncv]) {
            return null;
        }

        if (!this.ncvHeap.has(bucketName)) {
            this.ncvHeap.set(bucketName, new Map());
        }

        if (!this.ncvHeap.get(bucketName).has(version.Key)) {
            this.ncvHeap.get(bucketName).set(version.Key, new Map());
        }

        const ncvHeapObject = this.ncvHeap.get(bucketName).get(version.Key);

        const nncvSize = parseInt(rule[ncve][nncv], 10);

        const ruleId = rule[ncve].ID;

        if (!ncvHeapObject.get(ruleId)) {
            ncvHeapObject.set(ruleId, new MinHeap(nncvSize, noncurrentVersionCompare));
        }

        const heap = ncvHeapObject.get(ruleId);

        if (heap.size < nncvSize) {
            heap.add(version);
            return null;
        }

        const heapTop = heap.peek();
        if (noncurrentVersionCompare(version, heapTop) === CompareResult.LT) {
            return version;
        }

        const toExpire = heap.remove();
        heap.add(version);
        return toExpire;
    }

    /**
     * clear objects level entries from helper Heap object.
     * @param {string} bucketName - bucket name
     * @param {Set} uniqueObjectKeys - Set of unique object keys
     * @return {undefined} -
     */
    _ncvHeapObjectsClear(bucketName, uniqueObjectKeys) {
        if (!this.ncvHeap.has(bucketName)) {
            return;
        }

        const ncvHeapBucket = this.ncvHeap.get(bucketName);
        uniqueObjectKeys.forEach(key => {
            if (!ncvHeapBucket.has(key)) {
                return;
            }

            ncvHeapBucket.get(key).clear();
            ncvHeapBucket.delete(key);
        });
        return;
    }

    /**
     * clear bucket level entry from helper Heap object.
     * @param {string} bucketName - bucket name
     * @return {undefined} -
     */
    _ncvHeapBucketClear(bucketName) {
        if (!this.ncvHeap.has(bucketName)) {
            return;
        }

        const ncvHeapBucket = this.ncvHeap.get(bucketName);
        // remove references to Heap objects under each Key entries
        ncvHeapBucket.forEach(objMap => objMap.clear());
        // remove references to Key maps under each Bucket entries
        ncvHeapBucket.clear();
        // delete reference to bucket Map
        this.ncvHeap.delete(bucketName);
        return;
    }

    /**
     * Handles versioned objects (both enabled and suspended)
     * @param {object} bucketData - bucket data
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {string} versioningStatus - 'Enabled' or 'Suspended'
     * @param {number} nbRetries - Number of time the process has been retried
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _getObjectVersions(bucketData, bucketLCRules, versioningStatus, nbRetries, log, done) {
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
            const { error, sortedList: allVersions } = this._mergeSortedVersionsAndDeleteMarkers(
                data.Versions, data.DeleteMarkers, log,
            );
            if (error) {
                return done(error);
            }
            // for all versions and delete markers, add stale date property
            const allVersionsWithStaleDate = this._addStaleDateToVersions(
                bucketData.details, allVersions);

            // create Set of unique keys not matching the next marker to
            // indicate the object level entries to be cleared at the end
            // of the processing step
            const uniqueObjectKeysNotNextMarker = new Set();
            if (data.NextKeyMarker) {
                allVersions.forEach(v => {
                    if (v.Key !== data.NextKeyMarker) {
                        uniqueObjectKeysNotNextMarker.add(v.Key);
                    }
                });
            }

            // sending bucket entry - only once - for checking next listing
            if (data.IsTruncated && allVersions.length > 0 && nbRetries === 0) {
                // Uses last version whether Version or DeleteMarker
                const last = allVersions[allVersions.length - 1];
                const entry = Object.assign({}, bucketData, {
                    contextInfo: {
                        reqId: log.getSerializedUids(),
                    },
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
                allVersionsWithStaleDate, log, versioningStatus,
                err => {
                    if (err) {
                        return done(err);
                    }

                    if (!data.IsTruncated) {
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
     * Handles incomplete multipart uploads
     * @param {object} bucketData - bucket data
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {object} params - params for AWS S3 `listObjectVersions`
     * @param {number} nbRetries - Number of time the process has been retried
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _getMPUs(bucketData, bucketLCRules, params, nbRetries, log, done) {
        const req = this.s3target.listMultipartUploads(params);
        attachReqUids(req, log);
        async.waterfall([
            next => req.send((err, data) => {
                LifecycleMetrics.onS3Request(log, 'listMultipartUploads', 'bucket', err);

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
                if (data.IsTruncated && nbRetries === 0) {
                    // re-queue to kafka with `NextUploadIdMarker` &
                    // `NextKeyMarker` only once.
                    const entry = Object.assign({}, bucketData, {
                        contextInfo: {
                            reqId: log.getSerializedUids(),
                        },
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

    /** _decodeVID - decode the version id
     * @param {string} versionId - version ID
     * @param {Logger.newRequestLogger} log - logger object
     * @return {Object} result - { error, decodedVersionId }
     * @return {Error} result.error - if decoding failed
     * @return {String} result.decodedVersionId - decoded version id
     */
    _decodeVID(versionId, log) {
        if (versionId === 'null') {
            return { error: null, decodedVersionId: versionId };
        }

        const decoded = decode(versionId);
        if (decoded instanceof Error) {
            const invalidErr = errors.InternalError.customizeDescription('Invalid version id');
            log.error('error decoding version id', {
                method: 'LifecycleTask._decodeVID',
                error: invalidErr,
                versionId,
            });
            return { error: invalidErr, decodedVersionId: null };
        }

        return { error: null, decodedVersionId: decoded };
    }

    /**
     * Helper method to merge and sort Versions and DeleteMarkers lists
     * @param {array} versions - versions list
     * @param {array} deleteMarkers - delete markers list
     * @param {Logger.newRequestLogger} log - logger object
     * @return {object} result - { error, sortedList }
     * @return {Error} result.error - if decoding failed
     * @return {array} result.sortedList - merge sorted array
     */
    _mergeSortedVersionsAndDeleteMarkers(versions, deleteMarkers, log) {
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
                // The `Key` names of the versions and delete markers are the same.
                // Compare the `LastModified` timestamps of the versions and delete markers.
                const versionLastModified = new Date(versions[vIdx].LastModified);
                const deleteMarkerLastModified = new Date(deleteMarkers[dmIdx].LastModified);
                const isVersionLastModifiedNewer = versionLastModified > deleteMarkerLastModified;
                const isDMLastModifiedNewer = deleteMarkerLastModified > versionLastModified;

                if (isVersionLastModifiedNewer) {
                    sortedList.push(versions[vIdx++]);
                } else if (isDMLastModifiedNewer) {
                    sortedList.push(deleteMarkers[dmIdx++]);
                } else {
                    // If the version and the delete marker have the same last modified date
                    const nullVersion = (versions[vIdx].VersionId === 'null'
                    || deleteMarkers[dmIdx].VersionId === 'null');
                    // and either of them is a null version, we cannot decode the version ID,
                    // so we push the version object onto the sorted list first.
                    if (nullVersion) {
                        sortedList.push(versions[vIdx++]);
                    } else {
                        // and neither of them are null, we decode the version IDs and compare them.
                        // A lower version ID indicates a more recent version.
                        // What is the purpose of decoding the version ID?
                        // The version ID string is a combination of the current time in milliseconds
                        // and the position of the request in that millisecond.
                        // This means that two version IDs with the same last modified date can be sorted,
                        // because the position of the request is also included in the version ID.
                        const { error: decodeVidError, decodedVersionId } =
                            this._decodeVID(versions[vIdx].VersionId, log);
                        if (decodeVidError) {
                            return { error: decodeVidError, sortedList: null };
                        }

                        const { error: decodedDMError, decodedVersionId: decodedDMId } =
                            this._decodeVID(deleteMarkers[dmIdx].VersionId, log);
                        if (decodedDMError) {
                            return { error: decodedDMError, sortedList: null };
                        }

                        const isVersionVidNewer = decodedVersionId < decodedDMId;
                        if (isVersionVidNewer) {
                            sortedList.push(versions[vIdx++]);
                        } else {
                            sortedList.push(deleteMarkers[dmIdx++]);
                        }
                    }
                }
            }
        }

        return { error: null, sortedList };
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
            LifecycleMetrics.onS3Request(log, 'listObjectVersions', 'bucket', err);

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
     * Check that at least one rule has a tag or more.
     * @param {array} rules - array of rules
     * @return {boolean} true if at least one rule has a tag, false otherwise
     */
    _rulesHaveTag(rules) {
        return rules.some(rule => {
            if (!rule.Filter) {
                return false;
            }

            const tags = rule.Filter.And
                ? rule.Filter.And.Tags
                : (rule.Filter.Tag && [rule.Filter.Tag]);
            return tags && tags.length > 0;
        });
    }

    /**
     * Get object tagging if at least one lifecycle rule has a tag or more.
     * @param {object} tagParams - s3.getObjectTagging parameters
     * @param {string} tagParams.Bucket - bucket name
     * @param {string} tagParams.Key - object key name
     * @param {string} tagParams.VersionId - object version id
     * @param {array} bucketLCRules - array of bucket lifecycle rules
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} cb - callback(error, tags)
     * @return {undefined}
     */
    _getObjectTagging(tagParams, bucketLCRules, log, cb) {
        if (!this._rulesHaveTag(bucketLCRules)) {
            return process.nextTick(() => cb(null, { TagSet: [] }));
        }

        const req = this.s3target.getObjectTagging(tagParams);
        attachReqUids(req, log);
        return req.send((err, tags) => {
            LifecycleMetrics.onS3Request(log, 'getObjectTagging', 'bucket', err);
            if (err) {
                log.error('failed to get tags', {
                    method: 'LifecycleTask._getObjectTagging',
                    error: err,
                    bucket: tagParams.Bucket,
                    objectKey: tagParams.Key,
                    objectVersion: tagParams.VersionId,
                });
                return cb(err);
            }
            return cb(null, tags);
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
     * @param {object} object - object or object version
     * @param {Logger.newRequestLogger} log - logger object
     * @param {function} done - callback(error, data)
     * @return {undefined}
     */
    _getRules(bucketData, bucketLCRules, object, log, done) {
        if (this._isDeleteMarker(object)) {
            // DeleteMarkers don't have any tags, so avoid calling
            // `getObjectTagging` which will throw an error
            const filteredRules = this._lifecycleUtils.filterRules(bucketLCRules, object, []);
            return done(null, this._lifecycleUtils.getApplicableRules(filteredRules, object));
        }

        const tagParams = { Bucket: bucketData.target.bucket, Key: object.Key };
        if (object.VersionId) {
            tagParams.VersionId = object.VersionId;
        }

        return this._getObjectTagging(tagParams, bucketLCRules, log, (err, tags) => {
            if (err) {
                return done(err);
            }
            // tags.TagSet === [{ Key: '', Value: '' }, ...]
            const filteredRules = this._lifecycleUtils.filterRules(bucketLCRules, object, tags);
            // reduce filteredRules to only get earliest dates
            return done(null, this._lifecycleUtils.getApplicableRules(filteredRules, object));
        });
    }

    /**
     * check if rule applies for a given date or calculed days.
     * @param {array} rule - bucket lifecycle rule
     * @param {number} daysSinceInitiated - Days passed since entity (object or version) last modified
     * NOTE: entity is not an in-progress MPU or a delete marker.
     * @param {number} currentDate - current date
     * @return {boolean} true if rule applies - false otherwise.
     */
    _isRuleApplying(rule, daysSinceInitiated, currentDate) {
        if (rule.Expiration && this._supportedRules.includes('expiration')) {
            if (rule.Expiration.Days !== undefined && daysSinceInitiated >= rule.Expiration.Days) {
                return true;
            }

            if (rule.Expiration.Date && rule.Expiration.Date < currentDate) {
                return true;
            }
            // Expiration.ExpiredObjectDeleteMarker rule's action does not apply
            // since object is not a delete marker.
            // AbortIncompleteMultipartUpload.DaysAfterInitiation rule's action does not apply
            // since in-progress MPUs are beeing handled separetly prior to this checks.
        }

        if (rule.Transitions && rule.Transitions.length > 0
        && this._supportedRules.includes('transitions')) {
            return rule.Transitions.some(t => {
                if (t.Days !== undefined && daysSinceInitiated >= t.Days) {
                    return true;
                }
                if (t.Date && t.Date < currentDate) {
                    return true;
                }
                return false;
            });
        }

        return false;
    }

    /**
     * Check if entity (object or version) is eligible for expiration or transition based on its date.
     * This function was introduced to avoid having to go further into processing
     * (call getObjectTagging, headObject...) if the entity was not eligible based on its date.
     * @param {array} rules - array of bucket lifecycle rules
     * @param {object} entity - object or object version
     * NOTE: entity is not an in-progress MPU or a delete marker.
     * @param {string} versioningStatus - 'Enabled', 'Suspended', or 'Disabled'
     * @return {boolean} true if eligible - false otherwise.
     */
    _isEntityEligible(rules, entity, versioningStatus) {
        const currentDate = this._lifecycleDateTime.getCurrentDate();
        const daysSinceInitiated = this._lifecycleDateTime.findDaysSince(
            new Date(entity.LastModified)
        );
        const { staleDate } = entity;
        const daysSinceStaled = staleDate ?
            this._lifecycleDateTime.findDaysSince(new Date(staleDate)) : null;

        // Always eligible if object is a current version delete marker because
        // it requires extra s3 call (list versions).
        if (entity.IsLatest && this._isDeleteMarker(entity)) {
            return true;
        }

        return rules.some(rule => {
            if (rule.Status === 'Disabled') {
                return false;
            }

            if (versioningStatus === 'Enabled' || versioningStatus === 'Suspended') {
                if (entity.IsLatest) {
                    return this._isRuleApplying(rule, daysSinceInitiated, currentDate);
                }

                if (!staleDate) {
                    // NOTE: this should never happen. A non-current version should always have
                    // a stale date. If it is the case, we will log later for debug purposes.
                    return true;
                }

                if (rule.NoncurrentVersionExpiration
                && this._supportedRules.includes('noncurrentVersionExpiration')) {
                    if (rule.NoncurrentVersionExpiration.NoncurrentDays !== undefined &&
                    daysSinceStaled >= rule.NoncurrentVersionExpiration.NoncurrentDays) {
                        return true;
                    }
                }

                if (rule.NoncurrentVersionTransitions && rule.NoncurrentVersionTransitions.length > 0
                && this._supportedRules.includes('noncurrentVersionTransition')) {
                    return rule.NoncurrentVersionTransitions.some(t =>
                        (t.NoncurrentDays !== undefined && daysSinceInitiated >= t.NoncurrentDays));
                }

                return false;
            }

            return this._isRuleApplying(rule, daysSinceInitiated, currentDate);
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
            const eligible =
                this._isEntityEligible(lcRules, obj, versioningStatus);
            if (!eligible) {
                log.debug('entity is not eligible for lifecycle', {
                    bucket: bucketData.target.bucket,
                    key: obj.Key,
                    versionId: obj.VersionId,
                    isLatest: obj.IsLatest,
                    lastModified: obj.LastModified,
                    staleDate: obj.staleDate,
                    versioningStatus,
                });
                return process.nextTick(cb);
            }

            // We don't want to retry the _whole_ list if only a single entry fails,
            // so we possibly retry each individual entry here, and ignore errors.
            // Ignoring error is not too bad, the entry will be picked up again on
            // next lifecycle run
            return this._retryEntry({
                logFields: {
                    key: obj.Key,
                    versionId: obj.VersionId,
                    staleDate: obj.staleDate,
                    versioningStatus,
                },
                log,
                actionFunc: done => async.waterfall([
                    next => this._getRules(bucketData, lcRules, obj, log, next),
                    (applicableRules, next) => {
                        if (versioningStatus === 'Enabled' || versioningStatus === 'Suspended') {
                            return this._compareVersion(bucketData, obj, contents,
                                applicableRules, versioningStatus, log, next);
                        }
                        return this._compareObject(bucketData, obj, applicableRules, log,
                            next);
                    },
                ], done),
            }, cb);
        }, done);
    }

    /**
     * Retries processing an entry, respecting both individual and global retry
     * limits to avoid stalling the whole process.
     *
     * @param {Object} params - The parameters object.
     * @param {Function} params.actionFunc - The action function to retry.
     * @param {Object} params.log - The logger object.
     * @param {Object} params.logFields - The logger fields object.
     * @param {Function} cb - The callback function to execute after the retries.
     * @returns {void}
     */
    _retryEntry(params, cb) {
        const { actionFunc, log, logFields } = params;
        this.retry({
            actionDesc: 'compare rules lifecycle entry',
            logFields,
            log,
            actionFunc,
            maxRetries: MAX_RETRIES, // override maximum number of retries
            shouldRetryFunc: err => err.retryable && this._totalRetries < MAX_RETRIES_TOTAL,
            onRetryFunc: () => this._totalRetries++,
        }, () => {
            // To maintain consistency with the flow logic of LifecycleTask, no
            // errors will be returned from the callback.
            // This ensures that any failures during the looping process through
            // the keys (as seen in _compareRulesToList()) will not cause a
            // break in the loop.
            cb();
        });
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
        const daysSinceInitiated = this._lifecycleDateTime.findDaysSince(
            new Date(obj.LastModified)
        );
        const currentDate = this._lifecycleDateTime.getCurrentDate();

        if (rules.Expiration.Date &&
            rules.Expiration.Date < currentDate) {
            // expiration date passed for this object
            const entry = ActionQueueEntry.create('deleteObject')
                .addContext({
                    origin: 'lifecycle',
                    ruleType: 'expiration',
                    reqId: log.getSerializedUids(),
                })
                .setAttribute('target.owner', bucketData.target.owner)
                .setAttribute('target.bucket', bucketData.target.bucket)
                .setAttribute('target.accountId', bucketData.target.accountId)
                .setAttribute('target.key', obj.Key)
                .setAttribute('details.lastModified', obj.LastModified)
                .setAttribute('details.dataStoreName', obj.StorageClass || '')
                .setAttribute('transitionTime',
                    this._lifecycleDateTime.getTransitionTimestamp(
                        rules.Expiration, obj.LastModified)
                );
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
                .setAttribute('target.accountId', bucketData.target.accountId)
                .setAttribute('target.key', obj.Key)
                .setAttribute('details.lastModified', obj.LastModified)
                .setAttribute('details.dataStoreName', obj.StorageClass || '')
                .setAttribute('transitionTime',
                    this._lifecycleDateTime.getTransitionTimestamp(
                        rules.Expiration, obj.LastModified)
                );
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

    _getObjectMD(params, log, cb) {
        this.backbeatMetadataProxy.getMetadata(params, log, (err, blob) => {
            if (err) {
                log.error('failed to get object metadata', {
                    method: 'LifecycleTask._getObjectMD',
                    error: err,
                    bucket: params.bucket,
                    objectKey: params.objectKey,
                    versionId: params.versionId,
                });
                return cb(err);
            }
            const { error, result } = ObjectMD.createFromBlob(blob.Body);
            if (error) {
                const msg = 'error parsing metadata blob';
                return cb(errors.InternalError.customizeDescription(msg));
            }
            return cb(null, result);
        });
    }

    _putObjectMD(params, log, cb) {
        return this.backbeatMetadataProxy.putMetadata(params, log, err => {
            if (err) {
                log.error('failed to put object metadata', {
                    method: 'LifecycleTask._putObjectMD',
                    error: err,
                    bucket: params.bucket,
                    objectKey: params.objectKey,
                    versionId: params.versionId,
                });
                return cb(err);
            }
            return cb();
        });
    }

    _canUnconditionallyGarbageCollect(objectMD) {
        const sourceEndpoint = config.getBootstrapList()
            .find(endpoint => endpoint.site === objectMD.getDataStoreName());
        // Is it a local data source?
        if (!sourceEndpoint) {
            return true;
        }
        // Is the public cloud data source versioned?
        if (objectMD.getDataStoreVersionId()) {
            return true;
        }
        return false;
    }

    _getTransitionActionEntry(params, objectMD, log, cb) {
        let attempt;
        const umd = objectMD.getUserMetadata();
        if (umd) {
            const parsed = JSON.parse(umd);
            const rawAttempt = parsed['x-amz-meta-scal-s3-transition-attempt'];
            if (rawAttempt) {
                attempt = Number.parseInt(rawAttempt, 10);
            }
        }

        const entry = ReplicationAPI.createCopyLocationAction({
            bucketName: params.bucket,
            owner: params.owner,
            objectKey: params.objectKey,
            versionId: params.versionId,
            eTag: params.eTag,
            lastModified: params.lastModified,
            toLocation: params.site,
            originLabel: 'lifecycle',
            fromLocation: objectMD.getDataStoreName(),
            contentLength: objectMD.getContentLength(),
            resultsTopic: this.transitionTasksTopic,
            accountId: params.accountId,
            attempt,
        });
        entry.addContext({
            origin: 'lifecycle',
            ruleType: 'transition',
            reqId: log.getSerializedUids(),
        });

        if (this._canUnconditionallyGarbageCollect(objectMD)) {
            return cb(null, entry);
        }
        const locations = objectMD.getLocation();
        return this._headLocation(params, locations, log,
            (err, lastModified) => {
                if (err) {
                    return cb(err);
                }
                entry.setAttribute('source', {
                    bucket: params.bucket,
                    objectKey: params.objectKey,
                    storageClass: objectMD.getDataStoreName(),
                    lastModified,
                });
                return cb(null, entry);
        });
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
     * @param {string} params.lastModified - The last modified date of object
     * @param {string} params.site - The site name to transition the object to
     * @param {number} params.transitionTime - Unix time at which the transition should have occurred
     * @param {Werelogs.Logger} log - Logger object
     * @param {Function} [cb] - The callback to call
     * @return {undefined}
     */
    _applyTransitionRule(params, log, cb) {
        async.waterfall([
            next => {
                // No transition if lifecycle events to location were paused
                if (this.pausedLocations.has(params.site)) {
                    return next(errorLocationPaused);
                }
                return next();
            },
            next =>
                this._getObjectMD(params, log, (err, objectMD) => {
                    LifecycleMetrics.onS3Request(log, 'getMetadata', 'bucket', err);
                    return next(err, objectMD);
                }),
            (objectMD, next) => {
                // No transition when replication is ongoing or failed
                const replicationStatus = objectMD.getReplicationStatus();
                if (['PENDING', 'PROCESSING', 'FAILED'].includes(replicationStatus)) {
                    return next(errorReplicationInProgress);
                }
                const dataStoreName = objectMD.getDataStoreName();
                const isObjectCold = dataStoreName && locationsConfig[dataStoreName]
                    && locationsConfig[dataStoreName].isCold;
                // We do not transition cold objects
                if (isObjectCold) {
                    return next(errorTransitionColdObject);
                }
                // If transition is in progress, do not re-publish entry
                // to data-mover or cold-archive topic.
                if (objectMD.getTransitionInProgress()) {
                    return next(errorTransitionInProgress);
                }

                // If object is temporarily restored, don't try
                // to transition it again.
                const archive = objectMD.getArchive();
                if (archive && archive.restoreCompletedAt) {
                    return next(errorObjectTemporarilyRestored);
                }

                return this._getTransitionActionEntry(params, objectMD, log, (err, entry) =>
                    next(err, entry, objectMD));
            },
            (entry, objectMD, next) => {
                const locationName = params.site;
                const isCold = locationsConfig[locationName]?.isCold;
                LifecycleMetrics.onLifecycleTriggered(this.log, 'bucket',
                    isCold ? 'archive' : 'transition',
                    locationName, Date.now() - params.transitionTime);
                return ReplicationAPI.sendDataMoverAction(this.producer, entry, log,
                    err => next(err, entry, objectMD));
            },
            (entry, objectMD, next) => {
                // Update object metadata with "x-amz-scal-transition-in-progress"
                // to avoid transitioning object a second time from a new batch.
                // Only implemented for transitions to cold location.
                const toLocation = entry.getAttribute('toLocation');
                const locationConfig = locationsConfig[toLocation];
                if (locationConfig && locationConfig.isCold) {
                        objectMD.setTransitionInProgress(true);
                        const putParams = {
                            bucket: params.bucket,
                            objectKey: params.objectKey,
                            versionId: params.versionId,
                            mdBlob: objectMD.getSerialized(),
                        };
                        return this._putObjectMD(putParams, log, err => {
                            LifecycleMetrics.onS3Request(log, 'putMetadata', 'bucket', err);
                            return next(err);
                        });
                }

                return process.nextTick(next);
            }
        ], err => {
            if (err) {
                // FIXME: this can get verbose with expected errors
                // such as temporarily restored objects. A flag
                // needs to be added to expected errors.
                log.error('could not apply transition rule', {
                    method: 'LifecycleTask._applyTransitionRule',
                    error: err.description || err.message,
                    owner: params.owner,
                    bucket: params.bucket,
                    key: params.objectKey,
                    site: params.site,
                });
            } else {
                log.debug('transition rule applied', {
                    method: 'LifecycleTask._applyTransitionRule',
                    owner: params.owner,
                    bucket: params.bucket,
                    key: params.objectKey,
                    site: params.site,
                });
            }
            if (cb) {
                // NOTE: The purpose of introducing asynchronous logic here is to improve
                // the flow control of LifecycleTaskV2.
                cb(err);
            }
            return;
        });
    }

    /**
     * Helper method for NoncurrentVersionTransition.NoncurrentDays rule
     * Check if Noncurrent Transition rule applies on the version
     * @param {object} bucketData - bucket data
     * @param {object} version - single non-current version
     * @param {string} version.LastModified - last modified date of version
     * @param {object} rules - most applicable rules from `_getApplicableRules`
     * @param {Logger.newRequestLogger} log - logger object
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _checkAndApplyNCVTransitionRule(bucketData, version, rules, log, cb) {
        const staleDate = version.staleDate;
        const daysSinceInitiated = this._lifecycleDateTime.findDaysSince(new Date(staleDate));
        const ncvt = 'NoncurrentVersionTransition';
        const ncd = 'NoncurrentDays';
        const doesNCVTransitionRuleApply = (rules[ncvt] &&
            rules[ncvt][ncd] !== undefined &&
            daysSinceInitiated >= rules[ncvt][ncd]);

        if (doesNCVTransitionRuleApply) {
            this._applyTransitionRule({
                owner: bucketData.target.owner,
                accountId: bucketData.target.accountId,
                bucket: bucketData.target.bucket,
                objectKey: version.Key,
                versionId: version.VersionId,
                eTag: version.ETag,
                lastModified: version.LastModified,
                site: rules[ncvt].StorageClass,
                transitionTime: this._lifecycleDateTime.getTransitionTimestamp(
                    { Days: rules[ncvt][ncd] }, staleDate),
            }, log, cb);
            return;
        }

        if (cb) {
            // NOTE: The purpose of introducing asynchronous logic here is to improve the flow control
            // of LifecycleTaskV2.
            process.nextTick(cb);
        }
        return;
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
                            return next(err);
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
                const daysSinceInitiated = this._lifecycleDateTime.findDaysSince(
                    new Date(deleteMarker.LastModified)
                );

                const eodm = rules.Expiration &&
                    rules.Expiration.ExpiredObjectDeleteMarker;

                // Backbeat performs automatic ExpiredObjectDeleteMarker cleanup
                // for compatibility with Amazon S3,
                // - either when the delete markers meet the age criteria
                // - or when the ExpiredObjectDeleteMarker tag is set to true.
                const applicableExpRule = rules.Expiration && (
                    (rules.Expiration.Days !== undefined
                     && daysSinceInitiated >= rules.Expiration.Days)
                    || (rules.Expiration.Date !== undefined
                     && rules.Expiration.Date < Date.now())
                    || eodm === true
                );

                // if there are no other versions with the same Key as this DM and
                // if a valid Expiration rule exists, apply and permanently delete this DM
                if (matchingNoncurrentKeys.length === 0 && applicableExpRule) {
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
                                rules.Expiration, deleteMarker.LastModified)
                        );
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
     * @return {boolean} used to know if a rule has been applied already.
     *   If this rule has been applied to the object or version, other rules
     *   (i.e. transition) should not apply.
     */
    _checkAndApplyNCVExpirationRule(bucketData, version, rules, log) {
        const staleDate = version.staleDate;
        const daysSinceInitiated = this._lifecycleDateTime.findDaysSince(new Date(staleDate));
        const ncve = 'NoncurrentVersionExpiration';
        const ncd = 'NoncurrentDays';
        const nncv = 'NewerNoncurrentVersions';
        const doesNCVExpirationRuleApply = (rules[ncve] &&
            rules[ncve][ncd] !== undefined &&
            daysSinceInitiated >= rules[ncve][ncd]);
        if (doesNCVExpirationRuleApply) {
            let verToExpire = version;

            if (rules[ncve][nncv]) {
                log.debug('Rule contains NewerNoncurrentVersion. Checking heap for smallest', {
                    method: '_checkAndApplyNCVExpirationRule',
                    bucket: bucketData.target.bucket,
                    key: version.Key,
                });
                verToExpire = this._ncvHeapAdd(bucketData.target.bucket, rules, version);
                if (verToExpire === null) {
                    return false;
                }
            }

            const entry = ActionQueueEntry.create('deleteObject')
                .addContext({
                    origin: 'lifecycle',
                    ruleType: 'expiration',
                    reqId: log.getSerializedUids(),
                })
                .setAttribute('target.owner', bucketData.target.owner)
                .setAttribute('target.bucket', bucketData.target.bucket)
                .setAttribute('target.accountId', bucketData.target.accountId)
                .setAttribute('target.key', verToExpire.Key)
                .setAttribute('target.version', verToExpire.VersionId)
                .setAttribute('details.dataStoreName', verToExpire.StorageClass || '')
                .setAttribute('transitionTime',
                    this._lifecycleDateTime.getTransitionTimestamp(
                        { Days: rules[ncve][ncd] }, staleDate)
                );
            this._sendObjectAction(entry, err => {
                if (!err) {
                    log.debug('sent object entry for consumption',
                    Object.assign({
                        method: 'LifecycleTask._checkAndApplyNCVExpirationRule',
                    }, entry.getLogInfo()));
                }
            });
            return true;
        }
        return false;
    }

    _headLocation(params, locations, log, cb) {
        const headLocationParams = {
            bucket: params.bucket,
            objectKey: params.objectKey,
            locations,
        };
        this.backbeatMetadataProxy.headLocation(
            headLocationParams, log, (err, data) => {
                LifecycleMetrics.onS3Request(log, 'headLocation', 'bucket', err);
                if (err) {
                    log.error('error getting head response from CloudServer');
                    return cb(err);
                }
                return cb(null, data.lastModified);
            });
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
            LifecycleMetrics.onS3Request(log, 'headObject', 'bucket', err);

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
            if (rules.Expiration &&
                this._checkAndApplyExpirationRule(bucketData, object, rules, log)) {
                return done();
            }
            if (rules.Transition) {
                return this._applyTransitionRule({
                    owner: bucketData.target.owner,
                    accountId: bucketData.target.accountId,
                    bucket: bucketData.target.bucket,
                    objectKey: obj.Key,
                    eTag: obj.ETag,
                    lastModified: obj.LastModified,
                    site: rules.Transition.StorageClass,
                    transitionTime: this._lifecycleDateTime.getTransitionTimestamp(
                        rules.Transition, obj.LastModified),
                }, log, done);
            }

            return done();
        });
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

        if (rules.NoncurrentVersionExpiration &&
            this._checkAndApplyNCVExpirationRule(bucketData, version, rules, log)) {
            return done();
        }

        if (rules.NoncurrentVersionTransition) {
            return this._checkAndApplyNCVTransitionRule(bucketData, version, rules, log, done);
        }

        log.debug('no action taken on versioned object', {
            bucket: bucketData.target.bucket,
            key: version.Key,
            versioningStatus,
        });
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
        if (rules.Expiration &&
            this._checkAndApplyExpirationRule(bucketData, version, rules, log)) {
            return done();
        }
        if (rules.Transition) {
            return this._applyTransitionRule({
                owner: bucketData.target.owner,
                accountId: bucketData.target.accountId,
                bucket: bucketData.target.bucket,
                objectKey: version.Key,
                versionId: version.VersionId,
                eTag: version.ETag,
                lastModified: version.LastModified,
                site: rules.Transition.StorageClass,
                encodedVersionId: undefined,
                transitionTime: this._lifecycleDateTime.getTransitionTimestamp(
                    rules.Transition, version.LastModified),
            }, log, done);
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
            const filteredRules = this._lifecycleUtils.filterRules(bucketLCRules, upload, noTags);
            const aRules = this._lifecycleUtils.getApplicableRules(filteredRules, {});

            const daysSinceInitiated = this._lifecycleDateTime.findDaysSince(
                new Date(upload.Initiated)
            );
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
                    .setAttribute('target.accountId', bucketData.target.accountId)
                    .setAttribute('target.key', upload.Key)
                    .setAttribute('details.UploadId', upload.UploadId)
                    .setAttribute('details.dataStoreName', upload.StorageClass || '')
                    .setAttribute('transitionTime',
                        this._lifecycleDateTime.getTransitionTimestamp(
                            { Days: abortRule.DaysAfterInitiation }, upload.Initiated)
                    );
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
     * Skips kafka entry
     * @param {object} bucketData - bucket data from bucketTasksTopic
     * @param {Logger.newRequestLogger} log - logger object
     * @return {boolean} true if skipped, false otherwise.
     */
    _skipEntry(bucketData, log) {
        if (bucketData.details.listType) {
            log.debug('skip entry generated by the new lifecycle task', {
                method: 'LifecycleTask._skipEntry',
                bucketData,
            });
            return true;
        }
        return false;
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
     * @param {BackbeatMetadataProxy} backbeatMetadataProxy - The metadata proxy
     * @param {number} nbRetries - Number of time the process has been retried
     * @param {function} done - callback(error)
     * @return {undefined}
     */
    processBucketEntry(bucketLCRules, bucketData, s3target,
    backbeatMetadataProxy, nbRetries, done) {
        const log = this.log.newRequestLogger();
        this.s3target = s3target;
        this.backbeatMetadataProxy = backbeatMetadataProxy;
        if (!this.backbeatMetadataProxy) {
            return process.nextTick(done);
        }
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

        if (this._skipEntry(bucketData, log)) {
            return process.nextTick(done);
        }

        log.debug('processing bucket entry', {
            bucket: bucketData.target.bucket,
            owner: bucketData.target.owner,
            contextInfo: bucketData.contextInfo,
            details: bucketData.details,
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
                    mpuParams, nbRetries, log, cb);
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
                            LifecycleMetrics.onS3Request(
                                log,
                                'getBucketVersioning',
                                'bucket',
                                err
                            );

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
                        if (versioningStatus === 'Enabled'
                        || versioningStatus === 'Suspended') {
                            return this._getObjectVersions(bucketData,
                                bucketLCRules, versioningStatus, nbRetries, log, next);
                        }

                        return this._getObjectList(bucketData, bucketLCRules, nbRetries,
                            log, next);
                    },
                ], cb);
            },
        ], err => {
            log.info('finished processing task for bucket lifecycle', {
                method: 'LifecycleTask.processBucketEntry',
                bucket: bucketData.target.bucket,
                owner: bucketData.target.owner,
                contextInfo: bucketData.contextInfo,
                details: bucketData.details,
            });
            // An optimization is possible by only publishing when
            // finishing a complete bucket listing, let it aside for
            // simplicity as it is just updating a few zookeeper nodes
            this._snapshotDataMoverTopicOffsets(log);
            return done(err);
        });
    }
}

module.exports = LifecycleTask;
