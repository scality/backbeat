const fakeLogger = {
    trace: () => {},
    error: () => {},
    info: () => {},
    debug: () => {},
    getSerializedUids: () => {},
    end: () => fakeLogger,
};

module.exports = fakeLogger;
