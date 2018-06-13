const assert = require('assert');

const convertServiceStateFormat =
      require('../../../lib/management/convertServiceStateFormat');

describe('convertServiceStateFormat', () => {
    it('should convert a legacy state format into the new format', () => {
        const legacyState = {
            overlayVersion: 42,
            streams: {
                s1: {
                    streamId: 's1',
                    source: {
                        bucketName: 'b1',
                    },
                },
                s2: {
                    streamId: 's2',
                    source: {
                        bucketName: 'b2',
                    },
                },
                s3: {
                    streamId: 's3',
                    source: {
                        bucketName: 'b1',
                    },
                },
            },
        };
        const expectedConvState = {
            overlayVersion: 42,
            workflows: {
                b1: {
                    s1: {
                        workflowId: 's1',
                        source: {
                            bucketName: 'b1',
                        },
                    },
                    s3: {
                        workflowId: 's3',
                        source: {
                            bucketName: 'b1',
                        },
                    },
                },
                b2: {
                    s2: {
                        workflowId: 's2',
                        source: {
                            bucketName: 'b2',
                        },
                    },
                },
            },
        };

        const convState = convertServiceStateFormat(legacyState);
        assert.deepStrictEqual(convState, expectedConvState);
    });

    it('should keep the new state format as-is if nothing to convert', () => {
        const okState = {
            overlayVersion: 42,
            workflows: {
                b1: {
                    s1: {
                        workflowId: 's1',
                        source: {
                            bucketName: 'b1',
                        },
                    },
                    s3: {
                        workflowId: 's3',
                        source: {
                            bucketName: 'b1',
                        },
                    },
                },
                b2: {
                    s2: {
                        workflowId: 's2',
                        source: {
                            bucketName: 'b2',
                        },
                    },
                },
            },
        };
        const convState = convertServiceStateFormat(okState);
        assert.deepStrictEqual(convState, okState);
    });
});
