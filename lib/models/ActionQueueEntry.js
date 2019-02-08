const uuid = require('uuid/v4');

/**
 * @class
 * @classdesc Generic convenience class used to ask a remote service
 * to execute an action through message-passing, with the possibility
 * to retrieve the outcome in a separate message from a given topic.
 *
 * Here is an outline of the different workflows:
 *
 * When creating a new action object to query a service:
 *
 * - call create() to create the action object
 * - then use the various setters to set the required info
 * - optionally call setResultsTopic() in order to be notified of the
 *   outcome on this Kafka topic (if the service supports it)
 * - then serialize with toKafkaMessage()
 * - then send this serialized message to the service Kafka topic
 *
 * When receiving an action message from a Kafka queue to execute an
 * action:
 *
 * - call createFromKafkaEntry() to fetch the action from the queue
 * - then use the various getters to access the required info (action
 *   type and attributes) while executing the action
 * - then call one of the status setter when done (setEnd(),
 *   setSuccess() or setError()).
 * - then if getResultsTopic() gives a topic name, publish the outcome
 *   to this topic as the serialized action message returned by
 *   toKafkaMessage() (all services may not implement this)
 *
 * When receiving the outcome message of an action execution:
 *
 * - call createFromKafkaEntry() to fetch the action outcome from the
 *   queue
 * - then use getStatus() to know the outcome, then getResults() or
 *   getError() depending if the status is "success" or
 *   "error". Various getters may be used to fetch some of the
 *   original action attributes as well.
 */
class ActionQueueEntry {

    /**
     * Class method that creates an ActionQueueEntry to initiate a new
     * action.
     *
     * @param {string} actionType - predefined action type understood
     * by the action executor
     * @return {ActionQueueEntry} - a new instance of ActionQueueEntry
     */
    static create(actionType) {
        return new ActionQueueEntry({
            actionId: uuid(),
            action: actionType,
        });
    }

    /**
     * Class method that creates an ActionQueueEntry instance from a
     * kafka entry.
     *
     * Amongst other things, the returned action object will have a
     * start time set to the current time if not already set in the
     * action message, since in this case it's reasonable to assume
     * the action will be executed next.
     *
     * @param {Object} kafkaEntry - entry as read from Kafka queue
     * @return {ActionQueueEntry} - a new instance of ActionQueueEntry
     */
    static createFromKafkaEntry(kafkaEntry) {
        try {
            const record = JSON.parse(kafkaEntry.value);
            if (record.bootstrapId) {
                return { error: 'bootstrap entry' };
            }
            if (record.canary) {
                return { skip: 'skip canary entry' };
            }
            // let's be flexible and accept any valid JSON, as some
            // legacy action messages may not have all new attributes
            // (e.g. actionId)
            const action = new ActionQueueEntry(record);
            if (!action.processingStarted()) {
                action._startProcessing();
            }
            return action;
        } catch (err) {
            return { error: { message: 'malformed JSON in kafka entry',
                              description: err.message } };
        }
    }

    /**
     * Do not use this constructor directly, use the
     * ActionQueueEntry.create() method or
     * ActionQueueEntry.createFromKafkaEntry() instead.
     * @constructor
     * @param {object} actionAttributes - initial attributes
     */
    constructor(actionAttributes) {
        this._actionAttributes = actionAttributes;
        this._loggedAttributes = {
            actionId: 'actionId',
            actionType: 'action',
            actionContext: 'contextInfo',
            actionStatus: 'status',
            actionError: 'error',
        };
    }

    /**
     * Set or add an attribute to the action
     *
     * @param {string} attributePath - path to the attribute to set -
     * it can be a simple attribute name, or a dot-separated path to a
     * nested attribute. Missing parents are created automatically if
     * needed.
     * @param {object} value - attribute value, it can be any
     * serializable JS object (string, number, array or object)
     * @return {ActionQueueEntry} - this
     */
    setAttribute(attributePath, value) {
        if (value === undefined) {
            return this;
        }
        const attrPathElems = attributePath.split('.');
        let parentAttr = this._actionAttributes;
        for (let i = 0; i < attrPathElems.length - 1; ++i) {
            const attrPathElem = attrPathElems[i];
            if (parentAttr[attrPathElem] === undefined) {
                parentAttr[attrPathElem] = {};
            }
            parentAttr = parentAttr[attrPathElem];
        }
        const lastPathElem = attrPathElems[attrPathElems.length - 1];
        parentAttr[lastPathElem] = value;
        return this;
    }

    /**
     * Get the value of an action attribute
     *
     * @param {string} attributePath - path to the attribute to get -
     * it can be a simple attribute name, or a dot-separated path to a
     * nested attribute
     * @return {object|undefined} - attribute value, or {undefined} if
     * the attribute or any of its parents do not exist
     */
    getAttribute(attributePath) {
        const attrPathElems = attributePath.split('.');
        let attr = this._actionAttributes;
        for (let i = 0; i < attrPathElems.length; ++i) {
            const attrPathElem = attrPathElems[i];
            attr = attr[attrPathElem];
            if (attr === undefined) {
                return undefined;
            }
        }
        return attr;
    }

    /**
     * Add one or more action attributes to be returned by
     * getLogInfo(), in order to have them added to logs. Attributes
     * to be logged but not present in the action object will not be
     * logged.
     *
     * @param {object} attrMap - mapping of each attribute to be
     * logged, where keys are log attribute names, and values are
     * their matching action attribute path.
     *   E.g. <code>{ actionFooBar: 'foo.bar' }</code>
     * @return {ActionQueueEntry} - this
     */
    addLoggedAttributes(attrMap) {
        Object.assign(this._loggedAttributes, attrMap);
        return this;
    }

    getActionId() {
        return this.getAttribute('actionId');
    }

    getActionType() {
        return this.getAttribute('action');
    }

    /**
     * Add one or more context attributes from a JS object, that will
     * be stored in the "contextInfo" field of the action message and
     * logged as part of getLogInfo().
     *
     * @param {object} contextObj - attributes to add to the action context
     * @return {ActionQueueEntry} - this
     */
    addContext(contextObj) {
        if (!this._actionAttributes.contextInfo) {
            this._actionAttributes.contextInfo = {};
        }
        Object.assign(this._actionAttributes.contextInfo, contextObj);
        return this;
    }

    getContextAttribute(contextAttr) {
        return this.getAttribute(`contextInfo.${contextAttr}`);
    }

    getContext() {
        return this.getAttribute('contextInfo') || {};
    }

    /**
     * Set the name of the topic where the action results will be
     * forwarded to
     *
     * @param {string} resultsTopic - name of Kafka topic
     * @return {ActionQueueEntry} - this
     */
    setResultsTopic(resultsTopic) {
        return this.setAttribute('resultsTopic', resultsTopic);
    }

    getResultsTopic() {
        return this.getAttribute('resultsTopic');
    }

    /**
     * Mark the start time of the action execution to current time
     * (internal helper, called from
     * ActionQueueEntry.createFromKafkaEntry())
     *
     * @return {ActionQueueEntry} - this
     */
    _startProcessing() {
        return this.setAttribute('startTime', Date.now());
    }

    /**
     * Tell whether processing started or not
     *
     * @return {boolean} true if processing started (or ended)
     */
    processingStarted() {
        return this.getStartTime() !== undefined;
    }

    /**
     * Mark the end time of the action execution to current time
     * (internal helper, called from setSuccess() and setError())
     *
     * @return {ActionQueueEntry} - this
     */
    _endProcessing() {
        return this.setAttribute('endTime', Date.now());
    }

    /**
     * Tell whether processing is complete or not
     *
     * @return {boolean} true if processing has completed, false if it
     * has not completed (or not even started)
     */
    processingEnded() {
        return this.getEndTime() !== undefined;
    }

    /**
     * Return the execution status of the action, or undefined if
     * there is no status associated (i.e. action execution is not yet
     * complete)
     *
     * @return {string} "success" if operation was successful
     * (use getResults() to gather possible action outcome data),
     * or "error" if there was an error (use getError() to retrieve
     * info about the error)
     */
    getStatus() {
        return this.getAttribute('status');
    }

    /**
     * Calls setError(error) if error is defined and non-null,
     * otherwise calls setSuccess(resultsData)
     *
     * @param {Error|null} [error] - error object
     * @param {object} [resultsData] - data associated to action
     * outcome on success
     * @return {ActionQueueEntry} - this
     */
    setEnd(error, resultsData) {
        if (error) {
            return this.setError(error);
        }
        return this.setSuccess(resultsData);
    }

    /**
     * Set a success status after the action has executed correctly,
     * and optionally provide results data
     *
     * @param {object} [resultsData] - data associated to action outcome
     * @return {ActionQueueEntry} - this
     */
    setSuccess(resultsData) {
        return this._endProcessing()
            .setAttribute('status', 'success')
            .setAttribute('results', resultsData);
    }

    /**
     * Gather action outcome data after the action has executed
     * successfully (i.e. if getStatus() returns "success")
     *
     * @return {object} - results data
     */
    getResults() {
        return this.getAttribute('results');
    }

    /**
     * Set error state and info about an error that occurred when
     * processing the action
     *
     * @param {Error} [error] - an error object
     * @return {ActionQueueEntry} - this
     */
    setError(error) {
        return this._endProcessing()
            .setAttribute('status', 'error')
            .setAttribute('error.code', error && error.code)
            .setAttribute('error.message', error && error.message)
            .setAttribute('error.description', error && error.description);
    }

    getError() {
        return this.getAttribute('error');
    }

    getStartTime() {
        return this.getAttribute('startTime');
    }

    getEndTime() {
        return this.getAttribute('endTime');
    }

    /**
     * Get the time taken to execute the action
     *
     * @return {number|null} - number of milliseconds spent executing the action
     */
    getElapsedMs() {
        const startTime = this.getAttribute('startTime');
        const endTime = this.getAttribute('endTime');
        if (!(startTime && endTime)) {
            return undefined;
        }
        return endTime - startTime;
    }

    /**
     * Serialize the action into a JSON message suitable to be
     * published in Kafka for transmission to another process/service.
     *
     * @return {string} the JSON-serialized action message
     */
    toKafkaMessage() {
        return JSON.stringify(this._actionAttributes);
    }

    /**
     * Get a JS object suitable for logging useful info about the action
     *
     * @return {object} object containing attributes to be logged
     */
    getLogInfo() {
        const logInfo = {};
        Object.keys(this._loggedAttributes).forEach(logAttr => {
            const attrPath = this._loggedAttributes[logAttr];
            const attrValue = this.getAttribute(attrPath);
            if (attrValue !== undefined) {
                logInfo[logAttr] = attrValue;
            }
        });
        const elapsedMs = this.getElapsedMs();
        if (elapsedMs !== undefined) {
            // eslint-disable-next-line camelcase
            logInfo.elapsed_ms = elapsedMs;
        }
        return logInfo;
    }
}

module.exports = ActionQueueEntry;
