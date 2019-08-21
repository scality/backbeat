const assert = require('assert');
const WorkflowEngineDefs = require('workflow-engine-defs');
const ZookeeperMock = require('zookeeper-mock');
const NodeZookeeperClient = require('node-zookeeper-client');
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
    constructor(params) {
        super(params);
        this._state = {};
        this.zkClient = new ZookeeperMock();
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

    before(() => {
        const params = {
            config: {
                topic: TOPIC,
                zookeeperPath: ZKPATH
            },
            logger: fakeLogger,
            // logger:
            // new Logger('WorkflowEngineDataSourceQueuePopulator.spec.js')
        };
        wedsqp = new WorkflowEngineDataSourceQueuePopulatorMock(params);
    });

    afterEach(() => {
        wedsqp.resetState();
        wedsqp._deleteAllFilterDescriptors();
        wedsqp.zkClient._resetState();
    });

    it('should validate filter descriptor', () => {
        const fd = {
            workflowId: '24b0af11-a366-4e64-8c7a-7c637ec1b93b',
            workflowVersion: 42,
            type: 'put',
            subType: 'basic',
            bucket: 'foo',
            key: 'bar',
            nextNodes: []
        };
        const name = wedsqp._getHashString(
            fd.workflowId, fd.workflowVersion);
        const validationResult = wedsqp._loadFilterDescriptor(name, fd);
        assert(validationResult.isValid);
    });

    it('should not validate filter descriptor with missing fields', () => {
        const fd = {
            workflowId: '24b0af11-a366-4e64-8c7a-7c637ec1b93b',
            workflowVersion: 42,
            type: 'put',
            bucket: 'foo',
            key: 'bar',
            nextNodes: []
        };
        const name = wedsqp._getHashString(
            fd.workflowId, fd.workflowVersion);
        const validationResult = wedsqp._loadFilterDescriptor(name, fd);
        assert(!validationResult.isValid);
    });

    it('should not validate filter descriptor with wrong version', () => {
        const fd = {
            workflowId: '24b0af11-a366-4e64-8c7a-7c637ec1b93b',
            workflowVersion: 42,
            type: 'put',
            subType: 'basic',
            bucket: 'foo',
            key: 'bar',
            nextNodes: []
        };
        const name = wedsqp._getHashString(
            fd.workflowId, 43);
        const validationResult = wedsqp._loadFilterDescriptor(name, fd);
        assert(!validationResult.isValid);
    });

    it('should register a filter descriptor if added', done => {
        const fd = {
            workflowId: '24b0af11-a366-4e64-8c7a-7c637ec1b93b',
            workflowVersion: 42,
            type: 'put',
            subType: 'basic',
            bucket: 'foo',
            key: 'bar',
            nextNodes: []
        };
        const data = new Buffer(JSON.stringify(fd));
        const zkc = new ZookeeperMock();
        zkc.connect();
        assert(wedsqp._getFilterDescriptorsLength() === 0);
        const name = wedsqp._getHashString(
            fd.workflowId, fd.workflowVersion);
        zkc.create(
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
                    done();
                });
            });
    });

    it('should unregister a filter descriptor if deleted', done => {
        const fd = {
            workflowId: '24b0af11-a366-4e64-8c7a-7c637ec1b93b',
            workflowVersion: 42,
            type: 'put',
            subType: 'basic',
            bucket: 'foo',
            key: 'bar',
            nextNodes: []
        };
        const data = new Buffer(JSON.stringify(fd));
        const zkc = new ZookeeperMock();
        zkc.connect();
        assert(wedsqp._getFilterDescriptorsLength() === 0);
        const name = wedsqp._getHashString(
            fd.workflowId, fd.workflowVersion);
        zkc.create(
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
            desc: 'basic: accept right_bucket:undefined',
            workflow: overwriteScript(JSON.parse(JSON.stringify(dataWorkflow)),
                                      wed.SUB_TYPE_BASIC, undefined,
                                      'test-bucket-source', undefined),
            entry: Object.assign({}, {
                type: 'put',
                bucket: 'test-bucket-source',
                key: 'a-test-key2',
            }, { value: JSON.stringify(objectKafkaValue) }),
            results: { key: 'a-test-key2' },
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
            const fd = {
                workflowId: '24b0af11-a366-4e64-8c7a-7c637ec1b93b',
                workflowVersion: 42,
                type: 'put',
                subType: dataNodes[0].subType,
                bucket: dataNodes[0].key,
                key: dataNodes[0].value,
                nextNodes: wed.findNextNodes(dataNodes[0])
            };
            const data = new Buffer(JSON.stringify(fd));

            // simulate a client
            const zkc = new ZookeeperMock();
            zkc.connect();
            const name = wedsqp._getHashString(
                fd.workflowId, fd.workflowVersion);
            zkc.create(
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
