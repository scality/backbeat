'use strict';

// Operates on the MongoDB internal lifecycle format (lowercase ruleStatus,
// actions[], filter.rulePrefix) — NOT the AWS S3 format used by util/rules.js.

// A whole-bucket Expiration with days=0 is the explicit "empty this bucket"
// signal: treat it as mass-expiration so the conductor keeps lifecycle on v1.
function isMassExpirationRule(rule) {
    if (!rule || rule.ruleStatus !== 'Enabled') {
        return false;
    }
    if (rule.filter && (rule.filter.rulePrefix
            || (rule.filter.tags && rule.filter.tags.length > 0))) {
        return false;
    }
    return (rule.actions || []).some(action => action
        && action.actionName === 'Expiration'
        && action.days === 0);
}

function hasMassExpirationRule(rules) {
    return (rules || []).some(r => isMassExpirationRule(r));
}

module.exports = {
    isMassExpirationRule,
    hasMassExpirationRule,
};
