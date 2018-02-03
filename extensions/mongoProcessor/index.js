const MongoProcessorConfigValidator =
    require('./MongoProcessorConfigValidator');

module.exports = {
    name: 'generic',
    version: '1.0.0',
    configValidator: MongoProcessorConfigValidator,
};
