'use strict';

const assert = require('assert');

const MetadataPolicy =
    require('../../../extensions/mongoProcessor/metadataPolicy/MetadataPolicy');

describe('MetadataPolicy', () => {
    const policy = new MetadataPolicy();

    [
        'skipsStoredMetadata',
        'targetVersionId',
        'apply',
        'skipsDelete',
    ].forEach(method => it(`should refuse to ${method}() without an ` +
    'implementation', () => {
        assert.throws(() => policy[method](),
            new RegExp('sub-classes of MetadataPolicy must implement the ' +
                `${method}\\(\\) method`));
    }));
});
