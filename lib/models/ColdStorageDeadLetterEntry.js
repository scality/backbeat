const joi = require('joi');

const entrySchema = joi.object({
    op: joi.string().valid('restore', 'delete', 'archive').required(),
    bucketName: joi.string().required(),
    objectKey: joi.string().required(),
    objectVersion: joi.string(),
    accountId: joi.string().required(),
    archiveInfo: joi.object().required(),
    requestId: joi.string().required(),
});

class ColdStorageDeadLetterEntry {
    static createFromKafkaEntry(kafkaEntry) {
        try {
            const res = entrySchema.validate(JSON.parse(kafkaEntry.value));

            if (res.error) {
                return {
                    error: {
                        message: 'invalid status kafka entry',
                        description: res.error,
                    },
                };
            }

            const action = new ColdStorageDeadLetterEntry(res.value);
            return action;
        } catch (err) {
            return { error: { message: 'malformed JSON in kafka entry',
                              description: err.message } };
        }
    }

    constructor(attributes) {
        this._attributes = attributes;
    }

    get op() {
        return this._attributes.op;
    }

    get archiveInfo() {
        return this._attributes.archiveInfo;
    }

    get requestId() {
        return this._attributes.requestId;
    }

    get target() {
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

module.exports = ColdStorageDeadLetterEntry;
