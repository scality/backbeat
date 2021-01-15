const assert = require('assert');
const async = require('async');
const WorkflowEngineDefs = require('workflow-engine-defs');
const ZookeeperMock = require('zookeeper-mock');
const NodeZookeeperClient = require('node-zookeeper-client');
const uuid = require('uuid/v4');
// const werelogs = require('werelogs');

// const Logger = require('werelogs').Logger;

const WorkflowEngineDataSourceQueuePopulator =
      // eslint-disable-next-line
      require('../../../extensions/workflowEngineDataSource/WorkflowEngineDataSourceQueuePopulator');

const fakeLogger = require('../../utils/fakeLogger');

const TOPIC = 'fake-topic';
const ZKPATH = '/workflow-engine-data-source';

/**
 * This mock object is to overwrite the `publish` method and add a way of
 * getting information on published messages.
 * @class
 */
class WorkflowEngineDataSourceQueuePopulatorMock extends
WorkflowEngineDataSourceQueuePopulator {
    constructor(params, zk) {
        super(params);
        this._state = {};
        this.zkClient = zk.createClient();
        // artifically call class method
        this.createZkPath(err => {
            assert.ifError(err);
        }, true);
    }

    publish(topic, key, message) {
        assert.equal(topic, TOPIC);

        this._state.key = encodeURIComponent(key);
        this._state.message = message;
    }

    getState() {
        return this._state;
    }

    resetState() {
        this._state = {};
    }
}

function overwriteScript(obj, subType, script, key, value) {
    /* eslint-disable no-param-reassign */
    obj.nodes[0].subType = subType;
    obj.nodes[0].script = script;
    obj.nodes[0].key = key;
    obj.nodes[0].value = value;
    return obj;
    /* eslint-enable no-param-reassign */
}

describe('workflow engine queue populator', () => {
    let wedsqp;
    const wed = new WorkflowEngineDefs();
    const zk = new ZookeeperMock({ doLog: false });

    before(() => {
        const params = {
            config: {
                topic: TOPIC,
                zookeeperPath: ZKPATH
            },
            logger: fakeLogger,
        };
        wedsqp = new WorkflowEngineDataSourceQueuePopulatorMock(params, zk);
    });

    afterEach(() => {
        wedsqp.resetState();
        wedsqp._deleteAllFilterDescriptors();
        zk._resetState();
    });

    it('should validate filter descriptor', () => {
        const value = {
            workflowId: '24b0af11-a366-4e64-8c7a-7c637ec1b93b',
            workflowVersion: 42,
            subType: 'basic',
            bucket: 'foo',
            key: 'bar',
            nextNodes: []
        };
        const name = uuid();
        const validationResult = wedsqp._loadFilterDescriptor(name, value);
        assert(validationResult.isValid);
    });

    it('should not validate filter descriptor with missing fields', () => {
        const value = {
            workflowId: '24b0af11-a366-4e64-8c7a-7c637ec1b93b',
            workflowVersion: 42,
            bucket: 'foo',
            key: 'bar',
            nextNodes: []
        };
        const name = uuid();
        const validationResult = wedsqp._loadFilterDescriptor(name, value);
        assert(!validationResult.isValid);
    });

    it('should register a filter descriptor if added', done => {
        const value = {
            workflowId: '24b0af11-a366-4e64-8c7a-7c637ec1b93b',
            workflowVersion: 42,
            subType: 'basic',
            bucket: 'foo',
            key: 'bar',
            nextNodes: []
        };
        const fdName = wedsqp._getHashString(
            value.workflowId, value.workflowVersion);
        const data = Buffer.from(JSON.stringify(value));
        const zkc = zk.createClient();
        zkc.connect();
        assert(wedsqp._getFilterDescriptorsLength() === 0);
        const name = uuid();
        zkc.mkdirp(
            `${ZKPATH}/${name}`,
            data,
            {},
            NodeZookeeperClient.CreateMode.EPHEMERAL,
            err => {
                assert.ifError(err);
                // watchers are deactivated because asynchronous
                wedsqp._updateFilterDescriptors(err => {
                    assert.ifError(err);
                    assert(wedsqp._getFilterDescriptorsLength() === 1);
                    const fd = wedsqp.filterDescriptors[fdName];
                    assert(fd.refCount === 1);
                    done();
                });
            });
    });

    it('should unregister a filter descriptor if deleted', done => {
        const value = {
            workflowId: '24b0af11-a366-4e64-8c7a-7c637ec1b93b',
            workflowVersion: 42,
            subType: 'basic',
            bucket: 'foo',
            key: 'bar',
            nextNodes: []
        };
        const fdName = wedsqp._getHashString(
            value.workflowId, value.workflowVersion);
        const data = Buffer.from(JSON.stringify(value));
        const zkc = zk.createClient();
        zkc.connect();
        assert(wedsqp._getFilterDescriptorsLength() === 0);
        const name = uuid();
        zkc.mkdirp(
            `${ZKPATH}/${name}`,
            data,
            {},
            NodeZookeeperClient.CreateMode.EPHEMERAL,
            err => {
                assert.ifError(err);
                // watchers are deactivated because asynchronous
                wedsqp._updateFilterDescriptors(err => {
                    assert.ifError(err);
                    assert(wedsqp._getFilterDescriptorsLength() === 1);
                    const fd = wedsqp.filterDescriptors[fdName];
                    assert(fd.refCount === 1);
                    // destroy ephemeral nodes
                    zkc.close();
                    wedsqp._updateFilterDescriptors(err => {
                        assert.ifError(err);
                        assert(wedsqp._getFilterDescriptorsLength() === 0);
                        done();
                    });
                });
            });
    });

    it('refcounting shall work', done => {
        const value = {
            workflowId: '24b0af11-a366-4e64-8c7a-7c637ec1b93b',
            workflowVersion: 42,
            subType: 'basic',
            bucket: 'foo',
            key: 'bar',
            nextNodes: []
        };
        const fdName = wedsqp._getHashString(
            value.workflowId, value.workflowVersion);
        const data = Buffer.from(JSON.stringify(value));
        assert(wedsqp._getFilterDescriptorsLength() === 0);
        const zkc1 = zk.createClient();
        zkc1.connect();
        const zkc2 = zk.createClient();
        zkc2.connect();
        async.waterfall([
            next => {
                const name = uuid();
                zkc1.mkdirp(
                    `${ZKPATH}/${name}`,
                    data,
                    {},
                    NodeZookeeperClient.CreateMode.EPHEMERAL,
                    err => {
                        assert.ifError(err);
                        // watchers are deactivated because asynchronous
                        wedsqp._updateFilterDescriptors(err => {
                            assert.ifError(err);
                            assert(wedsqp._getFilterDescriptorsLength() === 1);
                            const fd = wedsqp.filterDescriptors[fdName];
                            assert(fd.refCount === 1);
                            return next(null);
                        });
                    });
            },
            next => {
                const name = uuid();
                zkc2.mkdirp(
                    `${ZKPATH}/${name}`,
                    data,
                    {},
                    NodeZookeeperClient.CreateMode.EPHEMERAL,
                    err => {
                        assert.ifError(err);
                        // watchers are deactivated because asynchronous
                        wedsqp._updateFilterDescriptors(err => {
                            assert.ifError(err);
                            assert(wedsqp._getFilterDescriptorsLength() === 1);
                            const fd = wedsqp.filterDescriptors[fdName];
                            assert(fd.refCount === 2);
                            return next(null);
                        });
                    });
            },
            next => {
                // destroy ephemeral nodes of first client
                zkc1.close();
                wedsqp._updateFilterDescriptors(err => {
                    assert.ifError(err);
                    // entry shall remain
                    assert(wedsqp._getFilterDescriptorsLength() === 1);
                    const fd = wedsqp.filterDescriptors[fdName];
                    assert(fd.refCount === 1);
                    return next(null);
                });
            },
            next => {
                // destroy ephemeral nodes of second client
                zkc2.close();
                wedsqp._updateFilterDescriptors(err => {
                    assert.ifError(err);
                    // entry shall disappear
                    assert(wedsqp._getFilterDescriptorsLength() === 0);
                    return next(null);
                });
            }
        ], err => {
            assert.ifError(err);
            return done();
        });
    });

    /* eslint-disable */
    const dataWorkflow = require('./DataWorkflow.json');

    const kafkaValue = {
        'owner-display-name': 'test_1522198049',
        'owner-id': 'e166a2080a0c2cf1474dce54654f3f224dd5ae01379f20f338d106b8bc964bb1',
        'content-length': 128,
        'content-md5': 'd41d8cd98f00b204e9800118ecf8427e',
        'x-amz-version-id': 'null',
        'x-amz-server-version-id': '',
        'x-amz-storage-class': 'STANDARD',
        'x-amz-server-side-encryption': '',
        'x-amz-server-side-encryption-aws-kms-key-id': '',
        'x-amz-server-side-encryption-customer-algorithm': '',
        'x-amz-website-redirect-location': '',
        acl: {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: []
        },
        replicationInfo: {
            content: [ 'DATA', 'METADATA' ]
        },
        key: '',
        location: null,
        isDeleteMarker: false,
        tags: {
            foo: 'bar'
        },
        dataStoreName: 'dc-1',
        'last-modified': '2018-03-28T22:10:00.534Z',
        'md-model-version': 3,
        versionId: '98477724999464999999RG001  1.30.12',
    };

    const objectKafkaValue = Object.assign({}, kafkaValue);
    const objectKafkaValue2 = JSON.parse(JSON.stringify(objectKafkaValue));
    objectKafkaValue2.tags[wed._GUARD] = true;
    objectKafkaValue2.tags['foo'] = 'bar';
    /* eslint-enable */

    [
        {
            desc: 'basic: accept right_bucket:*',
            workflow: overwriteScript(JSON.parse(JSON.stringify(dataWorkflow)),
                                      wed.SUB_TYPE_BASIC, undefined,
                                      'test-bucket-source', '*'),
            entry: Object.assign({}, {
                type: 'put',
                bucket: 'test-bucket-source',
                key: 'a-test-key',
            }, { value: JSON.stringify(objectKafkaValue) }),
            results: { key: 'a-test-key' },
        },
        {
            desc: 'basic: ignore wrong_bucket:*',
            workflow: overwriteScript(JSON.parse(JSON.stringify(dataWorkflow)),
                                      wed.SUB_TYPE_BASIC, undefined,
                                      'test-bucket-source2', '*'),
            entry: Object.assign({}, {
                type: 'put',
                bucket: 'test-bucket-source',
                key: 'a-test-key3',
            }, { value: JSON.stringify(objectKafkaValue) }),
            results: {},
        },
        {
            desc: 'basic: accept right_bucket:matching_object_regexp',
            workflow: overwriteScript(JSON.parse(JSON.stringify(dataWorkflow)),
                                      wed.SUB_TYPE_BASIC, undefined,
                                      'test-bucket-source', 'a-test-*'),
            entry: Object.assign({}, {
                type: 'put',
                bucket: 'test-bucket-source',
                key: 'a-test-key4',
            }, { value: JSON.stringify(objectKafkaValue) }),
            results: { key: 'a-test-key4' },
        },
        {
            desc: 'basic: ignore right_bucket:non_matching_object_regexp',
            workflow: overwriteScript(JSON.parse(JSON.stringify(dataWorkflow)),
                                      wed.SUB_TYPE_BASIC, undefined,
                                      'test-bucket-source', 'b-test-*'),
            entry: Object.assign({}, {
                type: 'put',
                bucket: 'test-bucket-source',
                key: 'a-test-key5',
            }, { value: JSON.stringify(objectKafkaValue) }),
            results: {},
        },
        {
            desc: 'ignore guarded entry',
            workflow: overwriteScript(JSON.parse(JSON.stringify(dataWorkflow)),
                                      wed.SUB_TYPE_BASIC, undefined,
                                      'test-bucket-source', 'a-test-key6'),
            entry: Object.assign({}, {
                type: 'put',
                bucket: 'test-bucket-source',
                key: 'a-test-key6',
            }, { value: JSON.stringify(objectKafkaValue2) }),
            results: {},
        },
    ].forEach(input => {
        it(`should filter entries properly: ${input.desc}`, done => {

            // find data source in input.workflow
            wed.setModel(input.workflow);
            const dataNodes = wed.findNodes(wed.TYPE_DATA);
            const validationResult = wed.checkModel();
            assert(validationResult.isValid);

            // register a filter descriptor
            const value = {
                workflowId: '24b0af11-a366-4e64-8c7a-7c637ec1b93b',
                workflowVersion: 42,
                subType: dataNodes[0].subType,
                bucket: dataNodes[0].key,
                key: dataNodes[0].value,
                nextNodes: wed.findNextNodes(dataNodes[0])
            };
            const data = Buffer.from(JSON.stringify(value));

            // simulate a client
            const zkc = zk.createClient();
            zkc.connect();
            const name = uuid();
            zkc.mkdirp(
                `${ZKPATH}/${name}`,
                data,
                {},
                NodeZookeeperClient.CreateMode.EPHEMERAL,
                err => {
                    assert.ifError(err);
                    // watchers are deactivated because asynchronous
                    wedsqp._updateFilterDescriptors(err => {
                        assert.ifError(err);
                        wedsqp.filter(input.entry);
                        const state = wedsqp.getState();
                        if (!Object.keys(state).length) {
                            assert(!Object.keys(input.results).length);
                        } else {
                            const entry = JSON.parse(state.message);
                            assert(entry.key === input.results.key);
                        }
                        done();
                    });
                });
        });
    });
});
