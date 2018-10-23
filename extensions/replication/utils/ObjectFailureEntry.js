const querystring = require('querystring');

/**
 * Class used to create an entry from the cached Redis key.
 */
class ObjectFailureEntry {
    /**
     * @constructor
     * @param {String} member - The Redis sorted set member for the failed
     * object. The key has the following schema: <bucket>:<key>:<versionId>
     * @param {String} sitename - The site in which the version is stored
     */
    constructor(member, sitename) {
        this.member = member;
        const schema = this.member.split(':');
        const [bucket, objectKey, encodedVersionId, encodedRole] = schema;
        this.bucket = bucket;
        this.objectKey = objectKey;
        this.encodedVersionId = encodedVersionId;
        this.sitename = sitename;
        this.encodedRole = encodedRole;
        this.role = encodedRole ? querystring.unescape(encodedRole) : undefined;
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
        return this.sitename;
    }

    getReplicationRoles() {
        return this.role;
    }

    getMember() {
        const member =
            `${this.bucket}:${this.objectKey}:${this.encodedVersionId}`;
        if (this.encodedRole) {
            return `${member}:${this.encodedRole}`;
        }
        return member;
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
