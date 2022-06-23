const joi = require('joi');

const entrySchema = joi.object({
    op: joi.string().valid('restore', 'delete', 'archive').required(),
    bucketName: joi.string().required(),
    objectKey: joi.string().required(),
    objectVersion: joi.string().required(),
    accountId: joi.string().required(),
    archiveInfo: joi.object({
        archiveId: joi.string().required(),
        archiveVersion: joi.string().required(),
    }).required(),
    requestId: joi.string().required(),
});

class ColdStorageActionQueueEntry {
    static createFromKafkaEntry(kafkaEntry) {
        const res = entrySchema.validate(kafkaEntry.value);

        if (res.error) {
            return {
                error: {
                    message: 'malformed JSON in kafka entry',
                    description: res.error,
                },
            };
        }

        const action = new ColdStorageActionQueueEntry(res.value);
        return action;
    }

    constructor(attributes) {
        this._attributes = attributes;
    }

    set(key, value) {
        this._attributes = value;
    }

    get(key) {
        return this._attributes[key];
    }

    getOp() {
        return this._attributes.op;
    }

    getArchiveInfo() {
        return this._attributes.archiveInfo;
    }

    getRequestId() {
        return this._attributes.requestId;
    }

    getBucketName() {
        return this._attributes.bucketName;
    }

    getObjectKey() {
        return this._attributes.objectKey;
    }

    getObjectVersion() {
        return this._attributes.objectVersion;
    }

    getTarget() {
        return {
            bucketName: this._attributes.bucketName,
            objectKey: this._attributes.objectKey,
            objectVersion: this._attributes.objectVersion,
            accountId: this._attributes.accountId,
        };
    }

    /**
     * Get a JS object suitable for logging useful info about the action
     *
     * @return {object} object containing attributes to be logged
     */
    getLogInfo() {
        return {
            op: this._attributes.op,
            requestId: this._attributes.requestId,
        };
    }
}

module.exports = ColdStorageActionQueueEntry;
