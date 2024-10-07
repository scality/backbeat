const { errors } = require('arsenal');

class BaseConfigManager {

    /**
     * Setup the config manager
     * @param {Function} cb callback
     * @return {undefined}
     */
    setup(cb) {
        return cb(errors.NotImplemented);
    }

    /**
     * Get bucket notification configuration
     * @param {String} bucket - bucket
     * @return {Object|undefined} - configuration if available or undefined
     */
    getConfig(bucket) { // eslint-disable-line no-unused-vars
        throw new errors.NotImplemented('Method not implemented');
    }

    /**
     * Set bucket notification configuration
     * @return {boolean} - false
     */
    setConfig() {
        throw new errors.NotImplemented('Method not implemented');
    }

    /**
     * Remove bucket notification configuration
     * @return {boolean} - false
     */
    removeConfig() {
        throw new errors.NotImplemented('Method not implemented');
    }
}

module.exports = BaseConfigManager;
