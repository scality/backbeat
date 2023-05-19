const { isMasterKey } = require('arsenal').versioning;
const { usersBucket, mpuBucketPrefix } = require('arsenal').constants;

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const ObjectQueueEntry = require('../../lib/models/ObjectQueueEntry');
const locationsConfig = require('../../conf/locationConfig.json') || {};

const { authTypeAssumeRole } = require('../../lib/constants');
// const { AccountIdCache } = require('../utils/AccountIdCache');
const VaultClientWrapper = require('../utils/VaultClientWrapper');

const REPLICATION_POPULATOR_CLIENT_ID = 'replication-populator';

class ReplicationQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.repConfig = params.config;
        this.metricsHandler = params.metricsHandler;

        this._authConfig = params.authConfig;

        this.vaultClientWrapper = new VaultClientWrapper(
            REPLICATION_POPULATOR_CLIENT_ID,
            params.vaultAdmin,
            this._authConfig,
            this.log,
        );

        if (this._authConfig.type === authTypeAssumeRole) {
            this.vaultClientWrapper.init();
        }

        // this._accountIdCache = new AccountIdCache(1000);
    }

    _getAccountId(ownerId, cb) {
        // if (this._accountIdCache.has(ownerId)) {
        //     return process.nextTick(cb, null, this._accountIdCache.get(ownerId));
        // }

        this.vaultClientWrapper.getAccountId(ownerId, (err, accountId) => {
            if (err) {
                if (err.NoSuchEntity) {
                    this.log.error('canonical id does not exist', { error: err, ownerId });
                    // this._accountIdCache.miss(ownerId);
                } else {
                    this.log.error('could not get account id', { error: err, ownerId });
                }
                return cb(err);
            }

            // this._accountIdCache.set(ownerId, accountId);
            // this._accountIdCache.expireOldest();
            return cb(null, accountId);
        });
    }

    filter(entry) {
        if (entry.key === undefined || entry.value === undefined) {
            // bucket updates have no key in raft log
            return undefined;
        }
        if (entry.bucket === usersBucket) {
            return this._filterBucketOp(entry);
        }
        return this._filterKeyOp(entry);
    }

    _filterBucketOp(entry) {
        if (entry.type !== 'put' ||
            entry.key.startsWith(mpuBucketPrefix)) {
            return;
        }
        // remove logReader to prevent circular stringify
        const publishedEntry = Object.assign({}, entry);
        delete publishedEntry.logReader;

        this.log.trace('publishing bucket replication entry',
                       { bucket: entry.bucket });
        this.publish(this.repConfig.topic,
                     entry.bucket, JSON.stringify(publishedEntry));
    }

    _filterKeyOp(entry) {
        if (entry.type !== 'put') {
            return;
        }
        const value = JSON.parse(entry.value);
        const queueEntry = new ObjectQueueEntry(entry.bucket,
                                                entry.key, value);
        const sanityCheckRes = queueEntry.checkSanity();
        if (sanityCheckRes) {
            return;
        }
        // Allow a non-versioned object if being replicated from an NFS bucket.
        // Or if the master key is of a non versioned object
        if (!this._entryCanBeReplicated(queueEntry)) {
            return;
        }
        if (queueEntry.getReplicationStatus() !== 'PENDING') {
            return;
        }
        const dataStoreName = queueEntry.getDataStoreName();
        const isObjectCold = dataStoreName && locationsConfig[dataStoreName]
            && locationsConfig[dataStoreName].isCold;
        // We do not replicate cold objects.
        if (isObjectCold) {
            return;
        }

        // remove logReader to prevent circular stringify
        const repSites = queueEntry.getReplicationInfo().backends;
        const content = queueEntry.getReplicationContent();
        const bytes = content.includes('DATA') ?
            queueEntry.getContentLength() : 0;

        // record replication metrics by site
        repSites.filter(entry => entry.status === 'PENDING')
            .forEach(backend => {
                this._incrementMetrics(backend.site, bytes);
            });

        // TODO: replication specific metrics go here
        this.metricsHandler.bytes(
            entry.logReader.getMetricLabels(),
            bytes
        );
        this.metricsHandler.objects(
            entry.logReader.getMetricLabels()
        );

        const publishedEntry = Object.assign({}, entry);
        delete publishedEntry.logReader;

        const ownerId = value['owner-id'];
        this._getAccountId(ownerId, (err, accountId) => {
            if (err) {
                this.log.error('unable to get account', { ownerId, err });
                return;
            }

            this.log.trace('publishing object replication entry',
                { entry: queueEntry.getLogInfo() });

            publishedEntry.accountId = accountId;

            this.publish(this.repConfig.topic,
                        `${queueEntry.getBucket()}/${queueEntry.getObjectKey()}`,
                        JSON.stringify(publishedEntry));
        });
    }

    /**
     * Filter if the entry is considered a valid master key entry.
     * There is a case where a single null entry looks like a master key and
     * will not have a duplicate versioned key. They are created when you have a
     * non-versioned bucket with objects, and then convert bucket to versioned.
     * If no new versioned objects are added for given object(s), they look like
     * standalone master keys. The `isNull` case is undefined for these entries.
     * Non-versioned objects if being replicated from an NFS bucket are also allowed
     * Null versions which are objects created after suspending versioning are allowed,
     * these only have a master object that has an internal versionId and a 'isNull' flag.
     * @param {ObjectQueueEntry} entry - raw queue entry
     * @return {Boolean} true if we should filter entry
     */
    _entryCanBeReplicated(entry) {
        const isMaster = isMasterKey(entry.getObjectVersionedKey());
        const isNFS = entry.getReplicationIsNFS();
        // single null entries will have a version id as undefined or null.
        // do not filter single null entries
        const isNonVersionedMaster = entry.getVersionId() === undefined;
        const isNullVersionedMaster = entry.getIsNull();
        if (isMaster && !isNFS && !isNonVersionedMaster && !isNullVersionedMaster) {
            this.log.trace('skipping master key entry');
            return false;
        }
        return true;
    }
}

module.exports = ReplicationQueuePopulator;
