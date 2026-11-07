const { errors } = require('@scality/arsenal');

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
    // eslint-disable-next-line no-unused-vars
    getConfig(bucket) {
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
