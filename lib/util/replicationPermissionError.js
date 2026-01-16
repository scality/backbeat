/**
 * Helper utilities for detecting and logging CRR permission errors.
 */

/**
 * Check if an error is an AccessDenied error
 * @param {Error} err - The error object
 * @returns {boolean} True if the error is an AccessDenied error
 */
function isAccessDeniedError(err) {
    return err && (err.code === 'AccessDenied');
}

/**
 * Get enhanced log fields for AccessDenied errors.
 * These fields provide context to help diagnose permission issues.
 *
 * @param {string} bucket - The source bucket name
 * @param {string} sourceRole - The CRR source role ARN
 * @returns {object} Log fields with contextual information
 */
function getAccessDeniedLogFields(bucket, sourceRole) {
    return {
        accessDeniedHint: 'Verify that the source role has the required ' +
            'permissions on the source bucket.',
        sourceRole,
        bucket,
    };
}

module.exports = {
    isAccessDeniedError,
    getAccessDeniedLogFields,
};
