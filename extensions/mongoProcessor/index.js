const MongoProcessorConfigValidator =
    require('./MongoProcessorConfigValidator');

module.exports = {
    name: 'ingestion',
    version: '1.0.0',
    configValidator: MongoProcessorConfigValidator,
};
