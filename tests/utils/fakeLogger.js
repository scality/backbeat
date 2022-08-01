const fakeLogger = {
    trace: () => {},
    error: console.error, // eslint-disable-line no-console
    info: console.log,    // eslint-disable-line no-console
    debug: console.debug, // eslint-disable-line no-console
    warn: console.warn,    // eslint-disable-line no-console
    getSerializedUids: () => {},
    end: () => fakeLogger,
};

module.exports = fakeLogger;
