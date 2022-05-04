const OplogPopulatorConfigValidator =
    require('./OplogPopulatorConfigValidator');

module.exports = {
    name: 'oplogPopulator',
    version: '1.0.0',
    configValidator: OplogPopulatorConfigValidator,
};
