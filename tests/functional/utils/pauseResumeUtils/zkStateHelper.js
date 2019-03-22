const async = require('async');
const zookeeper = require('node-zookeeper-client');

const EPHEMERAL_NODE = 1;

/**
 * @class ZKStateHelper
 *
 * @classdesc Helps to create zookeeper state based on the following state:
 *   - firstSite: not paused and no scheduled resume
 *   - secondSite: paused with scheduled resume set to `futureDate`
 */
class ZKStateHelper {
    constructor(zkConfig, statePath, firstSite, secondSite, futureDate) {
        this.zkConfig = zkConfig;
        this.statePath = statePath;
        this.futureDate = futureDate;

        this.zkClient = null;

        this.firstPath = `${this.statePath}/${firstSite}`;
        this.secondPath = `${this.statePath}/${secondSite}`;
    }

    getClient() {
        return this.zkClient;
    }

    get(site, cb) {
        const path = `${this.statePath}/${site}`;
        this.zkClient.getData(path, (err, data) => {
            if (err) {
                process.stdout.write('Zookeeper test helper error in ' +
                'ZKStateHelper.get at zkClient.getData');
                return cb(err);
            }
            let state;
            try {
                state = JSON.parse(data.toString());
            } catch (parseErr) {
                process.stdout.write('Zookeeper test helper error in ' +
                'ZKStateHelper.get at JSON.parse');
                return cb(parseErr);
            }
            return cb(null, state);
        });
    }

    set(site, state, cb) {
        const data = Buffer.from(state);
        const path = `${this.statePath}/${site}`;
        this.zkClient.setData(path, data, cb);
    }

    /**
     * Setup initial zookeeper state for pause/resume tests. After each test,
     * state should be reset to this initial state.
     * State is setup as such:
     *   - firstSite: { paused: false }
     *   - secondSite: { paused: true, scheduledResume: futureDate }
     * Where futureDate is defined at the top of this test file.
     * @param {function} cb - callback(err)
     * @return {undefined}
     */
    init(cb) {
        const { connectionString } = this.zkConfig;
        this.zkClient = zookeeper.createClient(connectionString);
        this.zkClient.connect();
        this.zkClient.once('connected', () => {
            async.series([
                next => this.zkClient.mkdirp(this.statePath, err => {
                    if (err && err.name !== 'NODE_EXISTS') {
                        return next(err);
                    }
                    return next();
                }),
                next => {
                    // emulate first site to be active (not paused)
                    const data =
                        Buffer.from(JSON.stringify({ paused: false }));
                    this.zkClient.create(this.firstPath, data, EPHEMERAL_NODE,
                        next);
                },
                next => {
                    // emulate second site to be paused
                    const data = Buffer.from(JSON.stringify({
                        paused: true,
                        scheduledResume: this.futureDate.toString(),
                    }));
                    this.zkClient.create(this.secondPath, data, EPHEMERAL_NODE,
                        next);
                },
            ], err => {
                if (err) {
                    process.stdout.write('Zookeeper test helper error in ' +
                    'ZKStateHelper.init');
                    return cb(err);
                }
                return cb();
            });
        });
    }

    reset(cb) {
        // reset state, just overwrite regardless of current state
        async.parallel([
            next => {
                const data = Buffer.from(JSON.stringify({
                    paused: false,
                    scheduledResume: null,
                }));
                this.zkClient.setData(this.firstPath, data, next);
            },
            next => {
                const data = Buffer.from(JSON.stringify({
                    paused: true,
                    scheduledResume: this.futureDate.toString(),
                }));
                this.zkClient.setData(this.secondPath, data, next);
            },
        ], err => {
            if (err) {
                process.stdout.write('Zookeeper test helper error in ' +
                'ZKStateHelper.reset');
                return cb(err);
            }
            return cb();
        });
    }

    close() {
        if (this.zkClient) {
            this.zkClient.close();
            this.zkClient = null;
        }
    }
}


module.exports = ZKStateHelper;
