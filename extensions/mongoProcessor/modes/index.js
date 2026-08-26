const IngestionMode = require('./IngestionMode');

const modes = {
    ingestion: IngestionMode,
};

module.exports = {
    modes,
    defaultMode: 'ingestion',
};
