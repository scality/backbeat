const IngestionMetadataPolicy = require('./IngestionMetadataPolicy');
const PullReplicationMetadataPolicy = require('./PullReplicationMetadataPolicy');

const metadataPolicies = {
    ingestion: IngestionMetadataPolicy,
    dr: PullReplicationMetadataPolicy,
};

module.exports = {
    metadataPolicies,
    defaultMode: 'ingestion',
};
