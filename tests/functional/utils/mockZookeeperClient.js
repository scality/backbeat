const ZookeeperMock = require('zookeeper-mock');
const sinon = require('sinon');
const zookeeper = require('node-zookeeper-client');

function mockZookeeperClient(options) {
    const endpoint = 'fake.endpoint:2181';
    const zk = new ZookeeperMock(options);
    const client = zk.createClient(endpoint);
    sinon.stub(zookeeper, 'createClient').returns(client);
}

module.exports = mockZookeeperClient;
