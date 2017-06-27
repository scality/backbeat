'use strict'; // eslint-disable-line

const assert = require('assert');

describe('backbeat config parsing and validation', () => {
    it('should parse correctly the default config', () => {
        const config = require('../../../conf/Config');
        assert.notStrictEqual(config, undefined);
    });
});
