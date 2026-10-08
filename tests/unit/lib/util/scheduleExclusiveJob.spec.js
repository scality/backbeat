const assert = require('assert');
const sinon = require('sinon');
const schedule = require('node-schedule');

const { makeExclusive, scheduleExclusiveJob } = require('../../../../lib/util/scheduleExclusiveJob');

describe('scheduleExclusiveJob', () => {
    let logger;

    beforeEach(() => {
        logger = { error: sinon.stub() };
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should skip calls made while the task is running', async () => {
        let release;
        const task = sinon.stub()
            .onFirstCall().callsFake(() => new Promise(resolve => { release = resolve; }))
            .resolves();
        const run = makeExclusive(task, logger);
        const first = run();
        await run();
        assert.strictEqual(task.callCount, 1);
        release();
        await first;
        await run();
        assert.strictEqual(task.callCount, 2);
    });

    it('should log a failure and run again on the next call', async () => {
        const task = sinon.stub().onFirstCall().rejects(new Error('boom')).resolves();
        const run = makeExclusive(task, logger);
        await run();
        assert(logger.error.calledOnce);
        assert.strictEqual(logger.error.firstCall.args[1].error, 'boom');
        await run();
        assert.strictEqual(task.callCount, 2);
    });

    it('should schedule the exclusive task on the cron rule', async () => {
        const task = sinon.stub().resolves();
        const scheduleStub = sinon.stub(schedule, 'scheduleJob').returns('job');
        assert.strictEqual(scheduleExclusiveJob('* * * * * *', task, logger), 'job');
        assert.strictEqual(scheduleStub.firstCall.args[0], '* * * * * *');
        await scheduleStub.firstCall.args[1]();
        assert(task.calledOnce);
    });
});
