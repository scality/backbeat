const GarbageCollectorConfigValidator =
          require('./GarbageCollectorConfigValidator');

module.exports = {
    name: 'gc',
    version: '1.0.0',
    configValidator: GarbageCollectorConfigValidator,
};
