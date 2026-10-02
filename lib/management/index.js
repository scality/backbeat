// The operator (zenko-operator) backend is the only management backend:
// MANAGEMENT_BACKEND is still set by deployments but no longer selects anything.
module.exports = require('./operatorBackend');
