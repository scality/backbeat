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
     * @returns {string|undefined} supported operation type
     */
    _getType(operationType) {
        switch (operationType) {
            case 'insert':
            case 'update':
            case 'replace':
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
            changeStreamDocument = {};
        }
        const streamObject = {
            timestamp: new Date(data.timestamp),
            db: changeStreamDocument.ns && changeStreamDocument.ns.coll,
            entries: [{
                key: changeStreamDocument.documentKey && changeStreamDocument.documentKey._id,
                type: this._getType(changeStreamDocument.operationType),
                value: changeStreamDocument.fullDocument && changeStreamDocument.fullDocument.value,
            }],
        };
        callback(null, streamObject);
    }
}

module.exports = ListRecordStream;
