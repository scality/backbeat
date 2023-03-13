const assert = require('assert');
const werelogs = require('werelogs');

const { rulesToParams } = require(
    '../../../extensions/lifecycle/util/rulesReducer');

describe('rulesReducer', () => {
    beforeEach(() => {
    });

    it('with no rule', done => {
        const versioningStatus = 'Disabled';
        const currentDate = '';
        const bucketLCRules = [];
        const bucketData = {
            target: {
                bucket: 'bucket1',
            }
        };
        const res = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
    });
});
