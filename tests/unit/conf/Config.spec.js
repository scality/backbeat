'use strict'; // eslint-disable-line

const assert = require('assert');

const joi = require('joi');

const config = require('../../../lib/Config');
const { authJoi, inheritedAuthJoi } = require('../../../lib/config/configItems.joi');

describe('backbeat config parsing and validation', () => {
    it('should parse correctly the default config', () => {
        assert.notStrictEqual(config, undefined);
    });

    describe('inherited auth', () => {
        const schema = joi.object({
            auth: authJoi.optional(),
            child: joi.object({
                auth: inheritedAuthJoi,
            }),
        });

        const authObject = {
            type: 'service',
            account: 'account1',
        };

        it('fail if auth missing in both parent and child', () => {
            const obj = {
                child: {},
            };

            assert(schema.validate(obj).error);
        });

        it('allow missing auth in child if defined in parent', () => {
            const obj = {
                auth: authObject,
                child: {},
            };

            return schema.validateAsync(obj);
        });

        it('allow missing auth in parent if defined in child', () => {
            const obj = {
                child: {
                    auth: authObject,
                },
            };

            return schema.validateAsync(obj);
        });

        it('allow auth in both parent and child', () => {
            const obj = {
                auth: authObject,
                child: {
                    auth: authObject,
                },
            };

            return schema.validateAsync(obj);
        });
    });
});
