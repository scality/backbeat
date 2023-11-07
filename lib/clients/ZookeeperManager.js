const zookeeper = require('node-zookeeper-client');
const EventEmitter = require('events');
const async = require('async');

class ZookeeperManager extends EventEmitter {
    /**
     * Constructs an instance of the ZookeeperManager.
     *
     * @param {string} connectionString - The connection string for the ZooKeeper server.
     * @param {Object} options - Configuration options for the ZooKeeper client.
     * @param {Logger} log - An instance of a logging utility.
     */
    constructor(connectionString, options, log) {
        super();

        this.connectionString = connectionString;
        this.log = log;
        this.options = options;
        this.client = null;

        this._connect();
    }

    /**
     * Establishes a connection to the ZooKeeper server.
     *
     * This method initializes a new ZooKeeper client using the provided connection string
     * and options. It sets up event listeners for various client states such as 'connected',
     * 'disconnected', and 'expired'. The method handles reconnection logic in case of
     * client expiration and forwards ZooKeeper events through this class's EventEmitter interface.
     *
     * If 'autoCreateNamespace' is set in the options and the base path does not exist in ZooKeeper,
     * this method tries to create the necessary path (namespace) in ZooKeeper.
     *
     * @method _connect
     * @return {undefined}
     */
    _connect() {
        // clean up exists client before reconnect
        if (this.client) {
            // close and clean up the existing ZooKeeper client connection.
            this.close();
            this.client.removeAllListeners();
        }

        this.client = zookeeper.createClient(this.connectionString, this.options);

        let nsIndex;
        let namespace;
        let rootZkClient;
        async.series([
            // Check zookeeper client is connected
            next => this.client.once('connected', next),
            // Create namespace if it exists and options.autoCreateNamespace is true
            next => {
                // TODO: ARTESCA-10337 The 'autoCreateNamespace' functionality is currently specific to
                // Artesca and may be removed in future versions once the Zenko Operator can handle base path creation.
                // Once removed, we can simply rely on the 'connected' state instead of the 'ready' state and
                // stop listening on 'error' event.
                if (this.options && this.options.autoCreateNamespace) {
                    nsIndex = this.connectionString.indexOf('/');
                    namespace = this.connectionString.slice(nsIndex);
                    if (nsIndex > -1 && namespace !== '/') {
                        return process.nextTick(next);
                    }
                }
                return process.nextTick(() => next({ event: 'ready' }));
            },
            // Check that the namespace is not already created.
            next => this.getData('/', err => {
                if (err && err.name !== 'NO_NODE') {
                    return next({ event: 'error', err });
                }
                // If not created (NO_NODE error), go to next step.
                if (err) {
                    return next();
                }
                return next({ event: 'ready' });
            }),
            // Since the namespace has not been created, connect to the root zookeeper client
            next => {
                const hostPort = this.connectionString.slice(0, nsIndex);
                rootZkClient = zookeeper.createClient(hostPort, this.options);
                rootZkClient.connect();
                rootZkClient.once('connected', () => next());
            },
            // Once connected, use the root zookeeper client to create the namespace
            next => rootZkClient.mkdirp(namespace, err => {
                rootZkClient.close();
                if (err && err.name !== 'NODE_EXISTS') {
                    return next({ event: 'error', err });
                }
                return next({ event: 'ready' });
            }),
        ], ({ event, err }) => {
            this.emit(event, err);
        });

        this.client.once('expired', () => {
            this.log.info('zookeeper client expired', {
                method: 'ZookeeperManager.once.expired',
            });
            // establish a new session with the ZooKeeper.
            this._connect();
        });

        this.client.on('state', (state) => {
            this.log.debug('zookeeper new state', {
                state,
                method: 'ZookeeperManager.on.state',
            });
        });

        // Forward ZooKeeper events
        this.client.on('connected', () => {
            this.emit('connected');
        });

        this.client.on('disconnected', () => {
            this.emit('disconnected');
        });

        this.client.connect();
    }

    // Forward ZooKeeper methods

    /**
     * Shutdown the client.
     * @method close
     * @return {undefined}
     */
    close() {
        this.client.close();
        return;
    }

    /**
     * Create a node with given path, data, acls and mode.
     *
     * @method create
     * @param {String} path The node path.
     * @param {Buffer} [data=undefined] The data buffer.
     * @param {Array} [acls=ACL.OPEN_ACL_UNSAFE] An array of ACL object.
     * @param {CreateMode} [mode=CreateMode.PERSISTENT] The creation mode.
     * @param {Function} callback The callback function.
     * @return {undefined}
     */
    create(path, data, acls, mode, callback) {
        this.client.create(path, data, acls, mode, callback);
        return;
    }

    /**
     * Check the existence of a node. The callback will be invoked with the
     * stat of the given path, or null if node such node exists.
     *
     * If the watcher function is provided and the call is successful (no error
     * from callback), a watcher will be placed on the node with the given path.
     * The watcher will be triggered by a successful operation that creates/delete
     * the node or sets the data on the node.
     *
     * @method exists
     * @param {String} path - The node path.
     * @param {Function} [watcher] - The watcher function.
     * @param {Function} callback - The callback function.
     * @return {undefined}
     */
    exists(path, watcher, callback) {
        this.client.exists(path, watcher, callback);
        return;
    }

    /**
     * For the given node path, retrieve the children list and the stat.
     *
     * If the watcher callback is provided and the method completes successfully,
     * a watcher will be placed the given node. The watcher will be triggered
     * when an operation successfully deletes the given node or creates/deletes
     * the child under it.
     *
     * @method getChildren
     * @param {String} path - The node path.
     * @param {Function} [watcher] - The watcher function.
     * @param {Function} callback - The callback function.
     * @return {undefined}
     */
    getChildren(path, watcher, callback) {
        this.client.getChildren(path, watcher, callback);
        return;
    }

    /**
     *
     * Retrieve the data and the stat of the node of the given path.
     *
     * If the watcher is provided and the call is successful (no error), a watcher
     * will be left on the node with the given path.
     *
     * The watch will be triggered by a successful operation that sets data on
     * the node, or deletes the node.
     *
     * @method getData
     * @param {String} path - The node path.
     * @param {Function} [watcher] - The watcher function.
     * @param {Function} callback  - The callback function.
     * @return {undefined}
     */
    getData(path, watcher, callback) {
        this.client.getData(path, watcher, callback);
        return;
    }

    /**
     * Returns the state of the client.
     *
     * @method getState
     * @return {State} the state of the client.
     */
    getState() {
        return this.client.getState();
    }

    /**
     * Create node path in the similar way of `mkdir -p`
     *
     *
     * @method mkdirp
     * @param {String} path - The node path.
     * @param {Buffer} [data=undefined] - The data buffer.
     * @param {Array} [acls=ACL.OPEN_ACL_UNSAFE] - The array of ACL object.
     * @param {CreateMode} [mode=CreateMode.PERSISTENT] - The creation mode.
     * @param {Function} callback - The callback function.
     * @return {undefined}
     */
    mkdirp(path, data, acls, mode, callback) {
        this.client.mkdirp(path, data, acls, mode, callback);
        return;
    }

    /**
     * Delete a node with the given path. If version is not -1, the request will
     * fail when the provided version does not match the server version.
     *
     * @method remove
     * @param {String} path - The node path.
     * @param {Number} [version=-1] - The version of the node.
     * @param {Function} callback - The callback function.
     * @return {undefined}
     */
    remove(path, version, callback) {
        this.client.remove(path, version, callback);
        return;
    }

    /**
     * Set the data for the node of the given path if such a node exists and the
     * optional given version matches the version of the node (if the given
     * version is -1, it matches any node's versions).
     *
     * @method setData
     * @param {String} path - The node path.
     * @param {Buffer} data - The data buffer.
     * @param {Number} [version=-1] - The version of the node.
     * @param {Function} callback - The callback function.
     * @return {undefined}
     */
    setData(path, data, version, callback) {
        this.client.setData(path, data, version, callback);
        return;
    }

    // Custom methods
    /**
     * Recursively removes a node and all of its children from ZooKeeper.
     *
     * @method removeRecur
     * @param {String} path - The path of the node to remove.
     * @param {Function} cb - The callback function.
     * @return {undefined}
     */
    removeRecur(path, cb) {
        return async.waterfall([
            next => this.getChildren(path, next),
            (children, stat, next) => async.eachLimit(
                children, 2,
                (child, done) => this.removeRecur(`${path}/${child}`, done),
                next),
            next => this.remove(path, -1, next),
        ], cb);
    }

    /**
     * Sets the data for a node or creates the node if it does not exist.
     *
     * This method attempts to set the data for a node at the specified path.
     * If the node does not exist (NO_NODE error), it creates the path with
     * the provided data. This is similar to a 'create or update' operation.
     *
     * @param {String} path - The path of the node.
     * @param {Buffer} data - Data to set on the node.
     * @param {Function} cb - Callback function. Called with an error argument
     *                        if an error occurs, otherwise null.
     * @return {undefined}
     */
    setOrCreate(path, data, cb) {
        this.setData(path, data, err => {
            if (err) {
                if (err.getCode() === zookeeper.Exception.NO_NODE) {
                    return this.mkdirp(path, err => {
                        if (err) {
                            return cb(err);
                        }
                        return this.setData(path, data, cb);
                    });
                }
                return cb(err);
            }
            return cb();
        });
    }
}

module.exports = ZookeeperManager;
