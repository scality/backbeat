'use strict'; // eslint-disable-line

const assert = require('assert');
const LifecycleBucketProcessor = require('../../../extensions/lifecycle/bucketProcessor/LifecycleBucketProcessor');
const BackbeatMetadataProxy = require('../../../lib/BackbeatMetadataProxy');

const zkConfig = {};
const kafkaConfig = {};
const lcConfig = { auth: {}, bucketProcessor: {} };
const repConfig = {};
const s3Config = { host: 'init.test.host', port: 8000 };

describe('LifecycleBucketProcessor', () => {
    describe('_getBackbeatClient', () => {
        let lbp;

        beforeEach(() => {
            lbp = new LifecycleBucketProcessor(zkConfig, kafkaConfig, lcConfig, repConfig, s3Config, 'http');
            lbp.credentialsManager = {
                getCredentials() {
                    return {
                        accessKeyId: 'ak0',
                        secretAccessKey: 'sk0',
                    };
                },
            };
        });

        it('should return an instance of BackbeatMetadataProxy', () => {
            const canonicalId = 'cid0';
            const accountId = 'id0';
            const client = lbp._getBackbeatClient(canonicalId, accountId);

            assert.ok(client instanceof BackbeatMetadataProxy, 'client is not an instance of BackbeatMetadataProxy');
        });

        it('should return an instance of BackbeatMetadataProxy after caching', () => {
            const canonicalId = 'cid0';
            const accountId = 'id0';
            lbp._getBackbeatClient(canonicalId, accountId);
            const client = lbp._getBackbeatClient(canonicalId, accountId);

            assert.ok(client instanceof BackbeatMetadataProxy, 'client is not an instance of BackbeatMetadataProxy');
        });
    });
});
