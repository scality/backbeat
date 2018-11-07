'use strict'; // eslint-disable-line

const assert = require('assert');
const async = require('async');
const zookeeper = require('node-zookeeper-client');

const jsutil = require('arsenal').jsutil;
const ProvisionDispatcher =
          require('../../../lib/provisioning/ProvisionDispatcher');

const ZK_TEST_PATH = '/tests/prov-test';

describe('provision dispatcher based on zookeeper recipes',
function testDispatch() {
    const zkConf = { connectionString: `localhost:2181${ZK_TEST_PATH}` };
    const provisionList = ['0', '1', '2', '3', '4', '5', '6', '7'];
    let clients = [];

    this.timeout(60000);

    before(done => {
        const zkClient = zookeeper.createClient('localhost:2181');
        zkClient.connect();
        zkClient.on('connected', () => {
            zkClient.mkdirp(ZK_TEST_PATH, err => {
                assert.ifError(err);
                const prov = new ProvisionDispatcher(zkConf);
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
        clients[0] = new ProvisionDispatcher(zkConf);
        clients[0].subscribe((err, items) => {
            assert.ifError(err);
            assert.deepStrictEqual(items, provisionList);
            done();
        });
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
            });
        }
        for (let i = 0; i < 10; ++i) {
            clients[i] = new ProvisionDispatcher(zkConf);
        }
        // register clients with a random wait time for each
        for (let i = 0; i < 10; ++i) {
            setTimeout(() => subscribeClient(i), Math.random() * 1000);
        }
    });

    it('should redispatch to remaining partitipants when some fail',
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
            });
        }
        for (let i = 0; i < 10; ++i) {
            clients[i] = new ProvisionDispatcher(zkConf);
        }
        // register clients with a random wait time for each
        for (let i = 0; i < 10; ++i) {
            setTimeout(() => subscribeClient(i), Math.random() * 1000);
        }
    });
});
