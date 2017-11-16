const zookeeper = require('node-zookeeper-client');

/**
 * wrapper around createClient() from node-zookeeper-client module,
 * with the following enhancements:
 *
 *  - emits a 'ready' event when the zookeeper client is ready
 *  - takes an additional option "autoCreateNamespace"
 *
 * @param {string} connectionString - connection string to zookeeper
 *   (e.g. 'localhost:2181/backbeat')
 * @param {object} [options] - an object to set the client
 *   options. Currently available options are:
 * @param {number} [options.sessionTimeout] - session timeout in
 *   milliseconds, defaults to 30 seconds.
 * @param {number} [options.spinDelay] - the delay (in milliseconds)
 *   between each connection attempts.
 * @param {number} [options.retries] - the number of retry attempts
 *   for connection loss exception.
 * @param {boolean} [options.autoCreateNamespace] - when true, ensure
 *   namespace (chroot) base path is created if not exists
 *
 * @return {node-zookeeper-client.Client} a zookeeper client handle
 */
function createClient(connectionString, options) {
    const zkClient = zookeeper.createClient(connectionString, options);
    console.log('zkClient in create client!!', zkClient)
    console.log("connectionString!!", connectionString)
    console.log("options!!", JSON.stringify(options))
    zkClient.once('connected', () => {
        console.log("zkClient once connected called!!")
        // for some reason zkClient.exists() does not return
        // NO_NODE when base path does not exist, hence use
        // getData() instead
        console.log("about to call zkClient get data!!")
        zkClient.getData('/', err => {
            console.log("in cb of zkclient.getdata!!", err)
            if (err && err.name !== 'NO_NODE') {
                return zkClient.emit('error', err);
            }
            // NO_NODE error and autoCreateNamespace is enabled
            if (err && options && options.autoCreateNamespace) {
                console.log("in autoCreateNamespace is enabled!!")
                const nsIndex = connectionString.indexOf('/');
                const hostPort = connectionString.slice(0, nsIndex);
                const namespace = connectionString.slice(nsIndex);
                const rootZkClient = zookeeper.createClient(hostPort, options);
                rootZkClient.connect();
                return rootZkClient.mkdirp(namespace, err => {
                    console.log("in mkdirp callback!!")
                    console.log("err!!", err)
                    console.log("namespace!!", namespace);
                    if (err && err.name !== 'NODE_EXISTS') {
                        return zkClient.emit('error');
                    }
                    return zkClient.emit('ready');
                });
            }
            console.log("autoCreatenamespace is disabled or other issue, just going to call ready")
            return zkClient.emit('ready');
        });
    });
    return zkClient;
}

module.exports = {
    createClient,
};
