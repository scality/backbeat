'use strict'; // eslint-disable-line

const assert = require('assert');
const config = require('../../../conf/Config');

describe('backbeat config parsing and validation', () => {
    it('should parse correctly the default config', () => {
        assert.notStrictEqual(config, undefined);
    });
});
