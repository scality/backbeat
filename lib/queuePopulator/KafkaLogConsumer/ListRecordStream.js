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
     * Parse kafka message's value field twice, as the Kafka-connect MongoDB
     * source connector will double stringify the value field
     * @param {string} blob sringified json
     * @returns {Object} parsed json
     */
    _parseKafkaMessageValue(blob) {
        try {
            const parsed = JSON.parse(JSON.parse(blob));
            return parsed;
        } catch (err) {
            this._logger.warn('Got invalid kafka message value field format', {
                method: 'ListRecordStream._parseJson',
                value: blob,
            });
            return {};
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
        const changeStreamDocument = this._parseKafkaMessageValue(data.value);
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
