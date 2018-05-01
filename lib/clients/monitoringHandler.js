const client = require('prom-client');

const collectDefaultMetrics = client.collectDefaultMetrics;
const crrOpCount = new client.Counter({
    name: 'crr_number_of_operations',
    help: 'CRR number of objects replicated(operations)',
});
const crrOpDone = new client.Counter({
    name: 'crr_number_of_operations_completed',
    help: 'CRR number of operations completed',
});
const crrBytesCount = new client.Counter({
    name: 'crr_number_of_bytes',
    help: 'CRR number of bytes',
});
const crrBytesDone = new client.Counter({
    name: 'crr_number_of_bytes_completed',
    help: 'CRR number of bytes completed',
});
const crrSiteCount = new client.Gauge({
    name: 'crr_number_of_sites',
    help: 'CRR number of sites',
});

module.exports = {
    client,
    collectDefaultMetrics,
    crrOpCount,
    crrOpDone,
    crrBytesCount,
    crrBytesDone,
    crrSiteCount,
};
