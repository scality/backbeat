'use strict'; // eslint-disable-line

const assert = require('assert');
const async = require('async');
const ZookeeperMock = require('zookeeper-mock');

const jsutil = require('arsenal').jsutil;
const ProvisionDispatcher =
          require('../../../lib/provisioning/ProvisionDispatcher');

const ZK_TEST_PATH = '/tests/prov-test';

const DO_RAND_DELAY = true;

describe('provision dispatcher based on zookeeper recipes',
function testDispatch() {
    const zkConf = { connectionString: `localhost:2181${ZK_TEST_PATH}` };
    const provisionList = ['0', '1', '2', '3', '4', '5', '6', '7'];
    let clients = [];

    this.timeout(60000);

    const zk = new ZookeeperMock({ doLog: false });

    before(done => {
        const zkClient = zk.createClient('localhost:2181');
        zkClient.connect();
        zkClient.on('connected', () => {
            zkClient.mkdirp(ZK_TEST_PATH, err => {
                assert.ifError(err);
                const prov = new ProvisionDispatcher(
                    zkConf, zk);
                prov.addProvisions(provisionList, done);
            });
        });
    });
    afterEach(done => {
        async.each(clients, (client, cb) => {
            if (client !== undefined) {
                client.unsubscribe(cb);
            } else {
                cb();
            }
        }, err => {
            assert.ifError(err);
            clients = [];
            done();
        });
    });

    it('should be given all provisions when alone', done => {
        const cbOnce = jsutil.once(done);
        clients[0] = new ProvisionDispatcher(
            zkConf, zk);
        clients[0].subscribe((err, items) => {
            assert.ifError(err);
            assert.deepStrictEqual(items, provisionList);
            cbOnce();
        }, false);
    });

    it('should recheck if missing a watcher event', done => {
        const cbOnce = jsutil.once(done);
        clients[0] = new ProvisionDispatcher(
            zkConf, zk);
        let times = 0;
        clients[0].subscribe((err, items) => {
            assert.ifError(err);
            if (times === 0) {
                // first time we shall have all provisions
                assert(items.length === 8);
                // simulate a watcher event loss
                const myPath =
                      clients[0]._client._basePath +
                      clients[0]._getMyPath();
                const result = clients[0]._client._getZNode(myPath);
                assert.ifError(result.err);
                result.parent.children[result.baseName].emitter.removeAllListeners();
                // introduce a new client
                clients[1] = new ProvisionDispatcher(
                    zkConf, zk);
                clients[1].subscribe(err => {
                    assert.ifError(err);
                }, false);
            } else {
                /* there might be various intermediate callbacks and
                   since we will not receive the watcher event we will
                   not be aware of the arrival of the new
                   ProvisionDispatcher, but after the timer we will
                   get notified, and we will get half of the
                   provisions 8/2 = 4 */
                if (items.length === 4) {
                    cbOnce();
                }
            }
            times++;
        }, true, 5000);
    });

    it('should spread the load across participants with 10 participants',
    done => {
        const subscriptionLists = [];
        let nSubscriptionLists = 0;

        function checkResult() {
            subscriptionLists.sort();
            const nbIdleOwners =
                      subscriptionLists.length - provisionList.length;
            for (let i = 0; i < subscriptionLists.length; ++i) {
                if (i >= nbIdleOwners) {
                    assert.deepStrictEqual(subscriptionLists[i],
                                           [provisionList[i - nbIdleOwners]]);
                } else {
                    assert.deepStrictEqual(subscriptionLists[i], []);
                }
            }
        }
        const checkOnce = jsutil.once(() => {
            checkResult();
            done();
        });
        function subscribeClient(i) {
            clients[i].subscribe((err, items) => {
                assert.ifError(err);
                if (subscriptionLists[i] === undefined) {
                    ++nSubscriptionLists;
                }
                subscriptionLists[i] = items;
                if (nSubscriptionLists === provisionList.length) {
                    // wait a bit to fail if there are new calls
                    // changing monitored provisions from this point
                    setTimeout(checkOnce, 5000);
                }
            }, false);
        }
        for (let i = 0; i < 10; ++i) {
            clients[i] = new ProvisionDispatcher(zkConf, zk);
            clients[i]._setDoRandDelay(DO_RAND_DELAY);
        }
        // register clients with a random wait time for each
        for (let i = 0; i < 10; ++i) {
            setTimeout(() => subscribeClient(i), Math.random() * 1000);
        }
    });

    it('should redispatch to remaining participants when some fail',
    done => {
        const subscriptionLists = new Array(10).fill([]);
        let nSubscriptionLists = 0;

        function checkRedispatch() {
            const remainingSortedLists = subscriptionLists.slice(2);
            remainingSortedLists.sort();
            assert.deepStrictEqual(remainingSortedLists,
                [['0'], ['1'], ['2'], ['3'],
                                    ['4'], ['5'], ['6'], ['7']]);
            // end of test
            done();
        }
        function redispatch() {
            // unsubscribing is equivalent to losing ephemeral nodes
            // after a client times out on zookeeper (e.g. in case of
            // crash)
            clients[0].unsubscribe(() => {});
            clients[1].unsubscribe(() => {});
            clients[0] = undefined;
            clients[1] = undefined;
            // wait some time and check that redispatch worked as expected
            setTimeout(checkRedispatch, 1000);
        }
        const redispatchOnce = jsutil.once(redispatch);
        function subscribeClient(i) {
            clients[i].subscribe((err, items) => {
                assert.ifError(err);
                if (subscriptionLists[i].length === 0) {
                    ++nSubscriptionLists;
                }
                subscriptionLists[i] = items;
                if (nSubscriptionLists === provisionList.length) {
                    // wait a bit to fail if there are new calls
                    // changing monitored provisions from this point
                    setTimeout(redispatchOnce, 1000);
                }
            }, false);
        }
        for (let i = 0; i < 10; ++i) {
            clients[i] = new ProvisionDispatcher(zkConf, zk);
            clients[i]._setDoRandDelay(DO_RAND_DELAY);
        }
        // register clients with a random wait time for each
        for (let i = 0; i < 10; ++i) {
            setTimeout(() => subscribeClient(i), Math.random() * 1000);
        }
    });
});
