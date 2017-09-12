const assert = require('assert');

const ReplicationTaskScheduler = require(
    '../../../extensions/replication/queueProcessor/ReplicationTaskScheduler');

describe('replication task scheduler', () => {
    it('should ensure serialization of updates to the same versioned object',
    done => {
        const taskScheduler = new ReplicationTaskScheduler(
            (entry, done) => {
                setTimeout(() => {
                    // eslint-disable-next-line no-param-reassign
                    entry.object.value = entry.setValueTo;
                    done();
                }, Math.random() * 100);
            });
        const objects = [];
        let doneCount = 0;
        function doneFunc() {
            ++doneCount;
            if (doneCount === objects.length * 10) {
                objects.forEach(obj => {
                    assert.strictEqual(obj.value, 9);
                });
                done();
            }
        }
        for (let i = 0; i < 10; ++i) {
            objects.push({ versionedKey: `key_with_version_${i}`,
                          value: -1 });
            // the following inner operations shall be executed in
            // order because they have the same versionedKey (passed
            // to taskScheduler.push())
            for (let j = 0; j < 10; ++j) {
                taskScheduler.push({ object: objects[i], setValueTo: j },
                                   objects[i].versionedKey, doneFunc);
            }
        }
    });
});
