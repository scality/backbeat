'use strict';

const assert = require('assert');
const joi = require('joi');
const sinon = require('sinon');
const config = require('../../../lib/Config');
const { Config } = require('../../../lib/Config');
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

describe('Site name', () => {
    let conf;

    beforeEach(() => {
        conf = new Config();
    });

    afterEach(() => {
        delete process.env.BOOTSTRAP_SITE_NAME;
    });

    it('should filter bootstrapList based on SITE_NAME', () => {
        process.env.BOOTSTRAP_SITE_NAME = 'test-site-2';
        const expectedBootstrapList = conf.bootstrapList.filter(item => item.site === 'test-site-2');
        const newConfig = new Config();
        assert.deepStrictEqual(newConfig.bootstrapList, expectedBootstrapList);
    });

    it('should not filter bootstrapList if SITE_NAME is not set', () => {
        const expectedBootstrapList = conf.bootstrapList;
        const newConfig = new Config();
        assert.deepStrictEqual(newConfig.bootstrapList, expectedBootstrapList);
    });
});


describe('Config', () => {
    describe('getReplicationSiteDestConfig', () => {
        let ogConfigFileEnv;

        before(() => {
            ogConfigFileEnv = process.env.BACKBEAT_CONFIG_FILE;
        });

        afterEach(() => sinon.restore());

        after(() => {
            if (ogConfigFileEnv) {
                process.env.BACKBEAT_CONFIG_FILE = ogConfigFileEnv;
            }
        });

        describe('bootstrapList server normalization', () => {
            let conf;

            before(() => {
                process.env.BACKBEAT_CONFIG_FILE = `${__dirname}/configs/replicationServersConfig.json`;
                conf = new Config();
            });

            it('should normalize server entries with default port 443 for https transport', () => {
                const entry = conf.bootstrapList.find(e => e.site === 'https-site');
                assert.deepStrictEqual(entry.servers, ['s3.example.com:443']);
            });

            it('should normalize server entries with default port 80 for http transport', () => {
                const entry = conf.bootstrapList.find(e => e.site === 'http-site');
                assert.deepStrictEqual(entry.servers, ['s3.example.com:80']);
            });

            it('should preserve explicit port in server entries', () => {
                const entry = conf.bootstrapList.find(e => e.site === 'explicit-port-site');
                assert.deepStrictEqual(entry.servers, ['s3.example.com:8443']);
            });

            it('should not modify endpoint without servers array', () => {
                const entry = conf.bootstrapList.find(e => e.site === 'aws-site');
                assert.strictEqual(entry.servers, undefined);
                assert.strictEqual(entry.type, 'aws_s3');
            });
        });


        describe('getReplicationSiteDestConfig', () => {
            it('should return replication site destination config', () => {
                process.env.BACKBEAT_CONFIG_FILE = `${__dirname}/configs/replicationMultiDestConfig.json`;
                const conf = new Config();
                const destConfig = conf.getReplicationSiteDestConfig('aws3');
                assert.deepStrictEqual(destConfig, {
                    transport: 'https',
                    auth: {
                        type: 'service',
                        account: 'service-replication-3',
                    },
                    replicationEndpoint: {
                        site: 'aws3',
                        type: 'aws_s3',
                    },
                });
            });

            it('should return default replication destination config when site one is not available', () => {
                process.env.BACKBEAT_CONFIG_FILE = `${__dirname}/configs/replicationMultiDestConfig.json`;
                const conf = new Config();
                sinon.stub(conf.extensions.replication, 'destination').value({
                    transport: 'https',
                    auth: {
                        type: 'service',
                        account: 'service-replication',
                    },
                    bootstrapList: [
                        { site: 'aws1', type: 'aws_s3' },
                        { site: 'aws2', type: 'aws_s3' },
                        { site: 'aws3', type: 'aws_s3' }
                    ]
                });
                const destConfig = conf.getReplicationSiteDestConfig('aws3');
                assert.deepStrictEqual(destConfig, {
                    transport: 'https',
                    auth: {
                        type: 'service',
                        account: 'service-replication',
                    },
                    replicationEndpoint: {
                        site: 'aws3',
                        type: 'aws_s3',
                    },
                });
            });
        });
    });
});
