const stream = require('stream');

class ListRecordStream extends stream.Transform {
    /**
     * @constructor
     * @param {Logger} logger logger
     */
    constructor(logger) {
        super({ objectMode: true });
        this._logger = logger;
    }

    /**
     * Maps change stream operation type to
     * backbeat supported types
     * @param {string} operationType mongo opetation type
     * @param {object|undefined} objectMd raw object metadata
     * @returns {string|undefined} supported operation type
     */
    _getType(operationType, objectMd) {
        switch (operationType) {
            case 'insert':
            case 'replace':
            case 'update':
                // When the object metadata contain the "deleted"
                // flag, it means that the operation is the update
                // we perform before the deletion of an object. We
                // perform the update to keep all the metadata in the
                // oplog. This update is what will be used by backbeat
                // as the delete operation so we put the type of operation
                // for this event to a delete.
                // Backbeat still receives the actual delete operations
                // but they are ignored as they don't contain any metadata.
                // The delete operations are kept in case we want to listen
                // to delete events comming from special collections other
                // than "bucket" collections.
                if (objectMd && objectMd.value
                    && objectMd.value.deleted) {
                    return 'delete';
                }
                return 'put';
            case 'delete':
                return 'delete';
            default:
                this._logger.warn('Got unsupported operation', {
                    method: 'ListRecordStream._getType',
                    operationType,
                });
                return undefined;
        }
    }

    /**
     * Get object metadata from oplog event
     * @param {ChangeStreamDocument} changeStreamDocument changeStreamDocument
     * @returns {string|undefined} stringified objectMd
     */
    _getObjectMd(changeStreamDocument) {
        // Metadata values from updateDescription are used instead of
        // fullDocument as they represent the exact changes that occured
        // to the object. We only need this for the update operation.

        // The fullDocument value contains a copy of the entire updated
        // document at a point in time after the update, this can cause issues
        // when the object gets deleted just after the update (only for updates).
        const updateDescriptionMd = changeStreamDocument.updateDescription
            && changeStreamDocument.updateDescription.updatedFields
            && changeStreamDocument.updateDescription.updatedFields.value;
        if (changeStreamDocument.operationType === 'update') {
            return JSON.stringify(updateDescriptionMd);
        }
        const fullDocumentMd = changeStreamDocument.fullDocument && changeStreamDocument.fullDocument.value;
        return JSON.stringify(fullDocumentMd);
    }

    /**
     * Formats change stream entries
     * @param {Object} data chunk of data
     * @param {Buffer} data.value message contents as a Buffer
     * In our case this contains the changeStreamDocument data
     * @param {Number} data.size size of the message, in bytes
     * @param {string} data.topic topic the message comes from
     * @param {Number} data.offset  offset the message was read from
     * @param {Number} data.partition partition the message was on
     * @param {string} data.key key of the message if present
     * @param {Number} data.timestamp timestamp of message creation
     * @param {string} encoding enconding of data
     * @param {Function} callback callback
     * @returns {undefined}
     */
    _transform(data, encoding, callback) {
        let changeStreamDocument;
        try {
            changeStreamDocument = JSON.parse(data.value.toString());
        } catch (error) {
            this._logger.warn('Got invalid kafka message value format', {
                method: 'ListRecordStream._transform',
                data,
            });
            // skipping the event
            return callback(null, null);
        }
        const objectMd = this._getObjectMd(changeStreamDocument);
        const opType = this._getType(changeStreamDocument.operationType, objectMd);
        const streamObject = {
            timestamp: new Date(data.timestamp),
            db: changeStreamDocument.ns && changeStreamDocument.ns.coll,
            entries: [{
                key: changeStreamDocument.documentKey && changeStreamDocument.documentKey._id,
                type: opType,
                value: objectMd,
            }],
        };
        return callback(null, streamObject);
    }
}

module.exports = ListRecordStream;
