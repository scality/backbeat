const assert = require('assert');

const convertOverlayFormat =
      require('../../../lib/management/convertOverlayFormat');

describe('convertOverlayFormat', () => {
    it('should convert a legacy conf format into the new format', () => {
        const legacyConf = {
            version: 42,
            locations: ['a', 'b'],
            replicationStreams: [{
                streamId: 's1', source: {
                    bucketName: 'b1',
                },
            }, {
                streamId: 's2', source: {
                    bucketName: 'b2',
                },
            }, {
                streamId: 's3', source: {
                    bucketName: 'b1',
                },
            }],
        };
        const expectedConvConf = {
            version: 42,
            locations: ['a', 'b'],
            workflows: {
                replication: {
                    b1: [{
                        workflowId: 's1',
                        source: {
                            bucketName: 'b1',
                        },
                    }, {
                        workflowId: 's3',
                        source: {
                            bucketName: 'b1',
                        },
                    }],
                    b2: [{
                        workflowId: 's2',
                        source: {
                            bucketName: 'b2',
                        },
                    }],
                },
            },
        };

        const convConf = convertOverlayFormat(legacyConf);
        assert.deepStrictEqual(convConf, expectedConvConf);
    });

    it('should convert a mix of legacy conf and new conf format into the ' +
    'new format', () => {
        const legacyConf = {
            version: 42,
            locations: ['a', 'b'],
            replicationStreams: [{
                streamId: 's1', source: {
                    bucketName: 'b1',
                },
            }, {
                streamId: 's2', source: {
                    bucketName: 'b2',
                },
            }, {
                streamId: 's3', source: {
                    bucketName: 'b1',
                },
            }],
            workflows: {
                lifecycle: {
                    b1: [{
                        workflowId: 's1',
                        expiration: {},
                    }, {
                        workflowId: 's2',
                        noncurrentVersionExpiration: {},
                    }],
                    b2: [{
                        workflowId: 's3',
                        transition: {},
                    }],
                },
            },
        };
        const expectedConvConf = {
            version: 42,
            locations: ['a', 'b'],
            workflows: {
                replication: {
                    b1: [{
                        workflowId: 's1',
                        source: {
                            bucketName: 'b1',
                        },
                    }, {
                        workflowId: 's3',
                        source: {
                            bucketName: 'b1',
                        },
                    }],
                    b2: [{
                        workflowId: 's2',
                        source: {
                            bucketName: 'b2',
                        },
                    }],
                },
                lifecycle: {
                    b1: [{
                        workflowId: 's1',
                        expiration: {},
                    }, {
                        workflowId: 's2',
                        noncurrentVersionExpiration: {},
                    }],
                    b2: [{
                        workflowId: 's3',
                        transition: {},
                    }],
                },
            },
        };

        const convConf = convertOverlayFormat(legacyConf);
        assert.deepStrictEqual(convConf, expectedConvConf);
    });

    it('should keep the new conf format as-is if nothing to convert', () => {
        const okConf = {
            version: 42,
            locations: ['a', 'b'],
            workflows: {
                lifecycle: {
                    b1: [{
                        workflowId: 's1',
                        expiration: {},
                    }, {
                        workflowId: 's2',
                        noncurrentVersionExpiration: {},
                    }],
                    b2: [{
                        workflowId: 's3',
                        transition: {},
                    }],
                },
            },
        };

        const convConf = convertOverlayFormat(okConf);
        assert.deepStrictEqual(convConf, okConf);
    });
});
