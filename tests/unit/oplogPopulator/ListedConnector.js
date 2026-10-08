const assert = require('assert');

const ListedConnector = require('../../../extensions/oplogPopulator/modules/ListedConnector');
const { connectorStartupTime } = require('../../../extensions/oplogPopulator/modules/resumeToken');

function resumeTokenAt(t, i = 0) {
    return JSON.stringify({ _data: `82${t.toString(16).padStart(8, '0')}${i.toString(16).padStart(8, '0')}04` });
}

describe('ListedConnector', () => {
    const startupTimeKey = 'startup.mode.timestamp.start.at.operation.time';

    describe('state', () => {
        const running = { state: 'RUNNING' };
        [
            ['running', { connector: running, tasks: [{ id: 0, state: 'RUNNING' }] }, 'running'],
            ['unassigned', { connector: { state: 'UNASSIGNED' }, tasks: [] }, 'running'],
            ['paused', { connector: { state: 'PAUSED' }, tasks: [{ id: 0, state: 'PAUSED' }] }, 'paused'],
            ['stopped', { connector: { state: 'STOPPED' }, tasks: [] }, 'paused'],
            ['failed', { connector: { state: 'FAILED' }, tasks: [{ id: 0, state: 'RUNNING' }] }, 'failed'],
            ['task failed', { connector: running, tasks: [{ id: 0, state: 'FAILED' }] }, 'failed'],
            ['status-less', undefined, 'running'],
        ].forEach(([desc, status, expected]) => {
            it(`should report a ${desc} connector as ${expected}`, () => {
                assert.strictEqual(new ListedConnector({ name: 'c', config: {}, status }).state, expected);
            });
        });

        it('should list its failed tasks', () => {
            const failed = { id: 1, state: 'FAILED', trace: 'boom' };
            const status = { connector: running, tasks: [{ id: 0, state: 'RUNNING' }, failed] };
            assert.deepStrictEqual(new ListedConnector({ name: 'c', config: {}, status }).failedTasks, [failed]);
            assert.deepStrictEqual(new ListedConnector({ name: 'c', config: {} }).failedTasks, []);
        });
    });

    describe('oplogTimeFrom', () => {
        const stopRequestedAt = { t: 1000, i: 7 };
        const oplogTime = (config, offsets, stopTime = stopRequestedAt) =>
            new ListedConnector({ name: 'c', config }).oplogTimeFrom(offsets, stopTime);
        const offsetAt = (ns, t, i) => ({ partition: { ns }, offset: { _id: resumeTokenAt(t, i) } });

        it('should use the earliest committed offset of the current partition', () => {
            const config = { 'offset.partition.name': 'partition-new' };
            const offsets = [offsetAt('partition-old', 10), offsetAt('partition-new', 50, 2)];
            assert.deepStrictEqual(oplogTime(config, offsets), { t: 50, i: 2 });
        });

        it('should use every partition when the connector has no partition name', () => {
            const offsets = [offsetAt('mongodb://h/db', 50), offsetAt('mongodb://h2/db', 40)];
            assert.deepStrictEqual(oplogTime({}, offsets), { t: 40, i: 0 });
        });

        it('should fall back to the configured start time without offsets of the current partition', () => {
            const config = {
                'offset.partition.name': 'partition-new',
                [startupTimeKey]: connectorStartupTime({ t: 60, i: 3 }),
            };
            assert.deepStrictEqual(oplogTime(config, [offsetAt('partition-old', 10)]), { t: 60, i: 3 });
        });

        it('should fall back to shortly before the stop without a known start time', () => {
            // a per-bucket connector start time is an ISO date, not parsed
            const config = { [startupTimeKey]: '2026-10-20T10:00:00.000Z' };
            assert.deepStrictEqual(oplogTime(config, []), { t: 880, i: 0 });
        });

        it('should return null when nothing is known', () => {
            assert.strictEqual(oplogTime({}, [], null), null);
        });
    });
});
