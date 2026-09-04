'use strict';

const assert = require('assert');

const ProcessorMode =
    require('../../../extensions/mongoProcessor/modes/ProcessorMode');

describe('ProcessorMode', () => {
    const mode = new ProcessorMode();

    [
        'needsExistingMetadata',
        'getChangedContent',
        'applyNewObjectMetadata',
        'replacesExistingMetadata',
        'mergeExistingMetadata',
        'resolveVersionId',
        'shouldProcessDelete',
    ].forEach(method => it(`should refuse to ${method}() without an ` +
    'implementation', () => {
        assert.throws(() => mode[method](),
            new RegExp('sub-classes of ProcessorMode must implement the ' +
                `${method}\\(\\) method`));
    }));
});
