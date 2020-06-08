const assert = require('assert');

const { versioning } = require('arsenal');
const { DbPrefixes } = versioning.VersioningConstants;

const LogReader = require('../../../../lib/queuePopulator/LogReader');

describe('LogReader', () => {
    let logReader;
    let filteredEntry;

    before(() => {
        logReader = new LogReader({
            extensions: [{
                filter: entry => { filteredEntry = entry; },
            }],
        });
    });

    [{
        desc: 'v0 master key',
        key: 'v0-master-key',
        processedKey: 'v0-master-key',
    }, {
        desc: 'v0 version key',
        key: 'v0-version-key\u0000version-id',
        processedKey: 'v0-version-key\u0000version-id',
    }, {
        desc: 'v1 master key',
        key: `${DbPrefixes.Master}v1-master-key`,
        processedKey: 'v1-master-key',
    }, {
        desc: 'v1 version key',
        key: `${DbPrefixes.Version}v1-version-key\u0000version-id`,
        processedKey: 'v1-version-key\u0000version-id',
    }].forEach(testCase => {
        it(`LogReader::_processLogEntry() should process entry with a ${testCase.desc}`, () => {
            logReader._processLogEntry(null, { db: 'db' }, {
                type: 'put',
                key: testCase.key,
                value: '{}',
            });
            assert.deepStrictEqual(filteredEntry, {
                type: 'put',
                bucket: 'db',
                key: testCase.processedKey,
                value: '{}',
            });
        });
    });
});
