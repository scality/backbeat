const { errors } = require('arsenal');
const joi = require('joi');

const paramsJoi = joi.object({
    logger: joi.object().required(),
    collection: joi.object().required(),
    handler: joi.function().required(),
    pipeline: joi.array().required(),
    throwOnError: joi.boolean().required(),
}).required();

/**
 * @class ChangeStream
 *
 * @classdesc Wrapper arround MongoDB change stream API
 */
class ChangeStream {

    /**
     * @constructor
     * @param {Object} params change stream wrapper config
     * @param {Object} params.logger logger object
     * @param {Collection} params.collection MongoDB collection to watch
     * @param {Object[]} params.pipeline change stream pipeline
     * @param {Function} params.handler change event handler function
     * @param {Boolean} params.throwOnError throw error if got change stream error event
     * @param {string} params.useStartAfter use `startAfter` (mongo 4.2+) or `resumeAfter` after error
     */
    constructor(params) {
        joi.attempt(params, paramsJoi);
        this._logger = params.logger;
        this._collection = params.collection;
        this._changeHandler = params.handler;
        this._pipeline = params.pipeline;
        this._throwOnError = params.throwOnError;
        this._resumeToken = null;
        this._changeStream = null;
        this._resumeField = params.useStartAfter ? 'startAfter' : 'resumeAfter';
    }

    /**
     * Handler for change stream error events
     * @returns {undefined}
     * @throws {InternalError}
     */
    async _handleChangeStreamErrorEvent() {
        this._logger.error('An error occured when listening to the change stream', {
            method: 'ChangeStream._handleChangeStreamErrorEvent',
        });
        // removing handlers for "change" and "error" events
        this._changeStream.removeListener('change', this._changeHandler);
        this._changeStream.removeListener('error', this._handleChangeStreamErrorEvent.bind(this));
        // closing change stream if not closed
        if (!this._changeStream.isClosed()) {
            await this._changeStream.close();
        }
        if (this._throwOnError) {
            throw errors.InternalError.customizeDescription('An error occured while reading the change stream');
        }
        // reseting change stream
        this.start();
    }

    /**
     * Establishes change stream and sets handlers for "change"
     * and "error" events
     * @returns {undefined}
     * @throws {InternalError}
     */
    start() {
        const changeStreamParams = { fullDocument: 'updateLookup' };
        if (this._resumeToken) {
            changeStreamParams[this._resumeField] = this._resumeToken;
        }
        try {
            this._changeStream = this._collection.watch(this._pipeline, changeStreamParams);
            this._changeStream.on('change', change => {
                // saving resume token
                this._resumeToken = change._id;
                // calling custom handler
                this._changeHandler(change);
            });
            this._changeStream.on('error', this._handleChangeStreamErrorEvent.bind(this));
            this._logger.debug('Change stream set', {
                method: 'ChangeStream._setMetastoreChangeStream',
            });
        } catch (err) {
            this._logger.error('Error while setting the change stream', {
                method: 'ChangeStream._setMetastoreChangeStream',
                error: err.message,
            });
            throw errors.InternalError.customizeDescription(err.message);
        }
    }
}

module.exports = ChangeStream;
