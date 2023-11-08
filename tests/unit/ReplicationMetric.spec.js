const assert = require('assert');

const ReplicationMetric =
    require('../../extensions/replication/ReplicationMetric');
const ActionQueueEntry = require('../../lib/models/ActionQueueEntry');

const mock = {
    bytes: 1,
    extension: 'a',
    type: 'b',
    site: 'c',
    bucketName: 'd',
    objectKey: 'e',
    versionId: 'f',
};

describe('ReplicationMetric', () => {
    let entry;
    let metric;
    let sentMessages;

    function setEntry() {
        entry = ActionQueueEntry
            .create()
            .setAttribute('target', {
                bucket: mock.bucketName,
                key: mock.objectKey,
                version: mock.versionId,
            });
    }

    function setReplicationMetric() {
        const producer = {
            send: messages => {
                messages.forEach(message => sentMessages.push(message));
            }
        };
        metric = new ReplicationMetric()
            .withProducer(producer)
            .withEntry(entry)
            .withSite(mock.site)
            .withObjectSize(mock.bytes)
            .withMetricType(mock.type)
            .withExtension(mock.extension);
    }

    beforeEach(() => {
        setEntry();
        setReplicationMetric();
        sentMessages = [];
    });

    it('::_createProducerMessage should create a message', () => {
        const data = JSON.parse(metric._createProducerMessage());
        Object.keys(mock)
            .forEach(key => assert.strictEqual(data[key], mock[key]));
    });

    it('::_isLifecycleAction should return false by default', () => {
        metric.withEntry(entry);
        assert.strictEqual(metric._isLifecycleAction(), false);
    });

    it('::_isLifecycleAction should return true when origin is lifecycle',
        () => {
            entry.setAttribute('contextInfo', {
                origin: 'lifecycle',
            });
            metric.withEntry(entry);
            assert.strictEqual(metric._isLifecycleAction(), true);
        });

    it('::publish should not send data to topic if lifecycle task', () => {
        entry.setAttribute('contextInfo', {
            origin: 'lifecycle',
        });
        metric.withEntry(entry);
        metric.publish();
        assert.strictEqual(sentMessages.length, 0);
    });

    it('::publish should send data to topic', () => {
        metric.publish();
        assert.strictEqual(sentMessages.length, 1);
        const data = JSON.parse(sentMessages[0].message);
        Object.keys(mock)
            .forEach(key => assert.strictEqual(data[key], mock[key]));
    });
});
