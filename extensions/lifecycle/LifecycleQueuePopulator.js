const { constants, errorUtils } = require('arsenal');
const { mpuBucketPrefix } = constants;
const QueuePopulatorExtension =
    require('../../lib/queuePopulator/QueuePopulatorExtension');
const http = require('http');
const https = require('https');
const uuid = require('uuid/v4');
const { ChainableTemporaryCredentials } = require('aws-sdk');
const safeJsonParse = require('./util/safeJsonParse');
const { authTypeAssumeRole } = require('../../lib/constants');
const CredentialsManager = require('../../lib/credentials/CredentialsManager');
const { LifecycleMetrics } = require('./LifecycleMetrics');
const LIFECYCLE_BUCKETS_ZK_PATH = '/data/buckets';
const LIFEYCLE_POPULATOR_CLIENT_ID = 'lifecycle:populator';
const METASTORE = '__metastore';

class LifecycleQueuePopulator extends QueuePopulatorExtension {

    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.config - extension-specific configuration object
     * @param {Logger} params.logger - logger object
     */
    constructor(params) {
        super(params);
        this._authConfig = params.config.auth;
        this._transport = this._authConfig.transport;

        if (this._transport === 'https') {
            this.stsAgent = new https.Agent({ keepAlive: true });
        } else {
            this.stsAgent = new http.Agent({ keepAlive: true });
        }
        this.credentialsManager = new CredentialsManager('lifecycle', this.logger);
        this._storeAWSCredentialsPromise();
    }

    /**
     * Pre-create the zookeeper path for bucket lifecycle nodes, if necessary.
     * @param {Function} cb - The callback function.
     * @return {undefined}
     */
    createZkPath(cb) {
        const { zookeeperPath } = this.extConfig;
        const path = `${zookeeperPath}${LIFECYCLE_BUCKETS_ZK_PATH}`;
        return this.zkClient.getData(path, err => {
            if (err) {
                if (err.name !== 'NO_NODE') {
                    this.log.error('could not get zookeeper node path', {
                        method: 'LifecycleQueuePopulator.createZkPath',
                        error: err,
                    });
                    return cb(err);
                }
                return this.zkClient.mkdirp(path, err => {
                    if (err) {
                        this.log.error('could not create path in zookeeper', {
                            method: 'LifecycleQueuePopulator.createZkPath',
                            zookeeperPath,
                            error: err,
                        });
                        return cb(err);
                    }
                    return cb();
                });
            }
            return cb();
        });
    }

    _getBucketNodeZkPath(attributes) {
        const { zookeeperPath } = this.extConfig;
        return `${zookeeperPath}${LIFECYCLE_BUCKETS_ZK_PATH}/` +
            `${attributes.owner}:${attributes.uid}:${attributes.name}`;
    }

    /**
     * Create a new zookeeper node for any bucket that has a lifecycle
     * configuration. Remove the node if the bucket is deleted or the
     * configuration is deleted.
     * @param {Object} attributes - bucket attributes from metadata log
     * @param {String} attributes.owner - canonical ID of the bucket owner
     * @param {String} attributes.name - bucket name
     * @param {String} attributes.uid - bucket unique UID
     * @return {undefined}
     */
    _updateZkBucketNode(attributes) {
        const path = this._getBucketNodeZkPath(attributes);
        // Remove existing node if deleting the bucket or its configuration.
        if (attributes.deleted ||
            attributes.lifecycleConfiguration === null) {
            return this.zkClient.remove(path, err => {
                if (err && err.name !== 'NO_NODE') {
                    this.log.error('could not remove zookeeper node', {
                        method: 'LifecycleQueuePopulator._updateZkBucketNode',
                        zkPath: path,
                        error: err,
                    });
                }
                if (!err) {
                    this.log.info(
                        'removed lifecycle zookeeper watch node for bucket',
                        { owner: attributes.owner,
                          bucket: attributes.name });
                }
                return undefined;
            });
        }
        return this.zkClient.create(path, err => {
            if (err && err.name !== 'NODE_EXISTS') {
                this.log.error('could not create new zookeeper node', {
                    method: 'LifecycleQueuePopulator._updateZkBucketNode',
                    zkPath: path,
                    error: err,
                });
            }
            if (!err) {
                this.log.info(
                    'created lifecycle zookeeper watch node for bucket',
                    { owner: attributes.owner,
                      bucket: attributes.name });
            }
            return undefined;
        });
    }

    _isBucketEntryFromBucketd(entry) {
        // raft log entries
        // {
        //   bucket: bucket name
        //   key: no key value
        //   value: bucket value
        // }
        return entry.key === undefined && entry.bucket !== METASTORE;
    }

    _isBucketEntryFromFileMD(entry) {
        // file md log entries
        // {
        //   bucket: METASTORE
        //   key: bucket name
        //   value: bucket value
        // }
        return entry.bucket === METASTORE &&
            !(entry.key && entry.key.startsWith(mpuBucketPrefix));
    }

    // directly manages temp creds lifecycle, not going through CredentialsManager,
    // as vaultclient does not use `AWS.Credentials` objects, and the same set
    // can be reused forever as the role is assumed in only one account
    _storeAWSCredentialsPromise() {
        const { sts, roleName, type } = this._authConfig;

        if (type !== authTypeAssumeRole) {
            return;
        }

        const stsWithCreds = this.credentialsManager.resolveExternalFileSync(sts);
        const stsConfig = {
            endpoint: `${this._transport}://${sts.host}:${sts.port}`,
            credentials: {
                accessKeyId: stsWithCreds.accessKey,
                secretAccessKey: stsWithCreds.secretKey,
            },
            region: 'us-east-1',
            signatureVersion: 'v4',
            sslEnabled: this._transport === 'https',
            httpOptions: { agent: this.stsAgent, timeout: 0 },
            maxRetries: 0,
        };

        // FIXME: works with vault 7.10 but not 8.3 (return 501)
        // https://scality.atlassian.net/browse/VAULT-238
        // new STS(stsConfig)
        //     .getCallerIdentity()
        //     .promise()
        this._tempCredsPromise =
            Promise.resolve({
                Account: '000000000000',
            })
            .then(res =>
                new ChainableTemporaryCredentials({
                    params: {
                        RoleArn: `arn:aws:iam::${res.Account}:role/${roleName}`,
                        RoleSessionName: 'backbeat-lc-vaultclient',
                        // default expiration: 1 hour,
                    },
                    stsConfig,
                }))
            .then(creds => {
                this._tempCredsPromiseResolved = true;
                return creds;
            })
            .catch(err => {
                if (err.retryable) {
                    const retryDelayMs = 5000;

                    this.logger.error('could not set up temporary credentials, retrying', {
                        retryDelayMs,
                        error: err,
                    });

                    setTimeout(() => this._storeAWSCredentialsPromise(), retryDelayMs);
                } else {
                    this.logger.error('could not set up temporary credentials', {
                        error: errorUtils.reshapeExceptionError(err),
                    });
                }
            });
    }

    _getAccountId(canonicalId, cb) {
        this._tempCredsPromise
            .then(creds => this._vaultClientCache.getClientWithAWSCreds(
                LIFEYCLE_POPULATOR_CLIENT_ID,
                creds,
            ))
            .then(client => client.enableIAMOnAdminRoutes())
            .then(client => {
                const opts = {};
                return client.getAccountIds([canonicalId], opts, (err, res) => {
                    LifecycleMetrics.onVaultRequest(
                        this.logger,
                        'getAccountId',
                        err
                    );

                    if (err) {
                        return cb(err);
                    }
                    return cb(null, res.message.body[canonicalId]);
                });
            })
            .catch(err => cb(err));
    }

    _handleRestoreOp(entry) {
        if (entry.type !== 'put' ||
            entry.key.startsWith(mpuBucketPrefix)) {
            return;
        }

        const value = JSON.parse(entry.value);

        const operation = value.originOp;
        if (operation !== 's3:ObjectRestore') {
            return;
        }

        const locationName = value.dataStoreName;

        // if object already restore nothing to do, S3 has already updated object MD
        const isObjectAlreadyRestored = value.archive
            && value.archive.restoreCompletedAt
            && new Date(value.archive.restoreWillExpireAt) >= new Date();

        if (!value.archive || !value.archive.restoreRequestedAt ||
            !value.archive.restoreRequestedDays || isObjectAlreadyRestored) {
            return;
        }

        // We would need to provide the object's bucket's account id as part of the kafka entry.
        // This account id would be used by Sorbet to assume the bucket's account role.
        // The assumed credentials will be sent and used by TLP server to put object version
        // to the specific S3 bucket.
        const ownerId = value['owner-id'];
        this._getAccountId(ownerId, (err, accountId) => {
            if (err) {
                this.log.error('unable to get account', {
                    ownerId,
                });
            }
            this.log.trace('publishing bucket replication entry', { bucket: entry.bucket });

            const topic = `cold-restore-req-${locationName}`;
            this.publish(
                topic,
                `${entry.bucket}/${entry.key}`,
                JSON.stringify({
                    bucketName: entry.bucket,
                    objectKey: entry.key,
                    objectVersion: value.versionId,
                    archiveInfo: value.archive.archiveInfo,
                    requestId: uuid(),
                    accountId
                }),
            );
        });
    }

    /**
     * Filter record log entries for those that are potentially relevant to
     * lifecycle.
     * @param {Object} entry - The record log entry from metadata.
     * @return {undefined}
     */
    filter(entry) {
        if (entry.type !== 'put') {
            return undefined;
        }

        this._handleRestoreOp(entry);

        if (this.extConfig.conductor.bucketSource !== 'zookeeper') {
            this.log.debug('bucket source is not zookeeper, skipping entry', {
                bucketSource: this.extConfig.conductor.bucketSource,
            });
            return undefined;
        }

        let bucketValue = {};
        if (this._isBucketEntryFromBucketd(entry)) {
            const parsedEntry = safeJsonParse(entry.value);
            if (parsedEntry.error) {
                this.log.error('could not parse raft log entry', {
                    value: entry.value,
                    error: parsedEntry.error,
                });
                return undefined;
            }
            const parsedAttr = safeJsonParse(parsedEntry.result.attributes);
            if (parsedAttr.error) {
                this.log.error('could not parse raft log entry attribute', {
                    value: entry.value,
                    error: parsedAttr.error,
                });
                return undefined;
            }
            bucketValue = parsedAttr.result;
        } else if (this._isBucketEntryFromFileMD(entry)) {
            const { error, result } = safeJsonParse(entry.value);
            if (error) {
                this.log.error('could not parse file md log entry',
                            { value: entry.value, error });
                return undefined;
            }
            bucketValue = result;
        }

        const { lifecycleConfiguration } = bucketValue;
        if (lifecycleConfiguration !== undefined) {
            return this._updateZkBucketNode(bucketValue);
        }
        return undefined;
    }
}

module.exports = LifecycleQueuePopulator;
