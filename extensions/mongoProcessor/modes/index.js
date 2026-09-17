const DRMode = require('./DRMode');
const IngestionMode = require('./IngestionMode');

const modes = {
    ingestion: IngestionMode,
    dr: DRMode,
};

module.exports = {
    modes,
    defaultMode: 'ingestion',
};
