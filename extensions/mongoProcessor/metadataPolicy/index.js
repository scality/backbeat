const IngestionMetadataPolicy = require('./IngestionMetadataPolicy');

const metadataPolicies = {
    ingestion: IngestionMetadataPolicy,
};

module.exports = {
    metadataPolicies,
    defaultMode: 'ingestion',
};
