// const fakeLogger = {
//     trace: () => {},
//     error: () => {},
//     info: () => {},
//     debug: () => {},
//     getSerializedUids: () => {},
//     end: () => {},
//     newRequestLogger: () => fakeLogger,
// };

class DummyRequestLogger {
    constructor() {
        this.ops = [];
        this.counts = {
            trace: 0,
            debug: 0,
            info: 0,
            warn: 0,
            error: 0,
            fatal: 0,
        };
        this.defaultFields = {};
    }

    trace(msg) {
        this.ops.push(['trace', [msg]]);
        this.counts.trace += 1;
    }

    debug(msg) {
        this.ops.push(['debug', [msg]]);
        this.counts.debug += 1;
    }

    info(msg) {
        this.ops.push(['info', [msg]]);
        this.counts.info += 1;
    }

    warn(msg) {
        this.ops.push(['warn', [msg]]);
        this.counts.warn += 1;
    }

    error(msg) {
        this.ops.push(['error', [msg]]);
        this.counts.error += 1;
    }

    fatal(msg) {
        this.ops.push(['fatal', [msg]]);
        this.counts.fatal += 1;
    }

    getSerializedUids() { // eslint-disable-line class-methods-use-this
        return 'dummy:Serialized:Uids';
    }

    addDefaultFields(fields) {
        Object.assign(this.defaultFields, fields);
    }

    end() {
        return this;
    }

    newRequestLogger() {
        return this;
    }
}

module.exports = new DummyRequestLogger();
