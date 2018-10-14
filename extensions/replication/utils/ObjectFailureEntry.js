/**
 * Class used to create an entry from the cached Redis key.
 */
class ObjectFailureEntry {
    /**
     * @constructor
     * @param {String} redisKey - The Redis key for the failed object. The key
     * has the following schema: bb:crr:failed:<bucket>:<key>:<versionId>:<site>
     * @param {String} role - The source IAM role used for authentication when
     * getting the source object's metadata from cloud server
     */
    constructor(redisKey, role) {
        this.redisKey = redisKey;
        const schema = this.redisKey.split(':');
        if (process.env.CI) {
            schema.shift(); // Test keys are prefixed with 'test:'.
        }
        const [service, extension, status, bucket, objectKey, encodedVersionId,
            site] = schema;
        this.service = service;
        this.extension = extension;
        this.status = status;
        this.bucket = bucket;
        this.objectKey = objectKey;
        this.encodedVersionId = encodedVersionId;
        this.site = site;
        this.sourceRole = role;
    }

    getRedisKey() {
        return this.redisKey;
    }

    getService() {
        return this.service;
    }

    getExtension() {
        return this.extension;
    }

    getStatus() {
        return this.status;
    }

    getBucket() {
        return this.bucket;
    }

    getObjectKey() {
        return this.objectKey;
    }

    getEncodedVersionId() {
        return this.encodedVersionId;
    }

    getSite() {
        return this.site;
    }

    getReplicationRoles() {
        return this.sourceRole;
    }

    getLogInfo() {
        return {
            bucket: this.getBucket(),
            objectKey: this.getObjectKey(),
            encodedVersionId: this.getEncodedVersionId(),
        };
    }
}

module.exports = ObjectFailureEntry;
